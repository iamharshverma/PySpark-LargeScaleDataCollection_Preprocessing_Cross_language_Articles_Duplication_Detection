import pymongo
import ufal.udpipe
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession

total_processed = 0
class Model:
    def __init__(self, path):
        """Load given model."""
        self.model = ufal.udpipe.Model.load(path)
        if not self.model:
            raise Exception("Cannot load UDPipe model from file '%s'" % path)

    def tokenize(self, text):
        """Tokenize the text and return list of ufal.udpipe.Sentence-s."""
        tokenizer = self.model.newTokenizer(self.model.DEFAULT)
        if not tokenizer:
            raise Exception("The model does not have a tokenizer")
        return self._read(text, tokenizer)

    def read(self, text, in_format):
        """Load text in the given format (conllu|horizontal|vertical) and return list of ufal.udpipe.Sentence-s."""
        input_format = ufal.udpipe.InputFormat.newInputFormat(in_format)
        if not input_format:
            raise Exception("Cannot create input format '%s'" % in_format)
        return self._read(text, input_format)

    def _read(self, text, input_format):
        input_format.setText(text)
        error = ufal.udpipe.ProcessingError()
        sentences = []

        sentence = ufal.udpipe.Sentence()
        while input_format.nextSentence(sentence, error):
            sentences.append(sentence)
            sentence = ufal.udpipe.Sentence()
        if error.occurred():
            raise Exception(error.message)

        return sentences

    def tag(self, sentence):
        """Tag the given ufal.udpipe.Sentence (inplace)."""
        self.model.tag(sentence, self.model.DEFAULT)

    def parse(self, sentence):
        """Parse the given ufal.udpipe.Sentence (inplace)."""
        self.model.parse(sentence, self.model.DEFAULT)

    def write(self, sentences, out_format):
        """Write given ufal.udpipe.Sentence-s in the required format (conllu|horizontal|vertical)."""

        output_format = ufal.udpipe.OutputFormat.newOutputFormat(out_format)
        output = ''
        for sentence in sentences:
            output += output_format.writeSentence(sentence)
        output += output_format.finishDocument()

        return output


def udpipe_process(url, text):
    root = str("/Users/harshverma/PycharmProjects/Big_Data_Final_Project")
    input_data_path_spanish_model = root + "/Data/spanish_model/"
    db = pymongo.MongoClient("mongodb://localhost:27017/")["BDMA_PROJ"]["news_parsed"]
    model = Model(input_data_path_spanish_model + 'spanish-gsd-ud-2.3-181115.udpipe')

    sentences = model.tokenize(text)
    for sentence in sentences:
        model.tag(sentence)
        model.parse(sentence)

    conllu = model.write(sentences, "conllu").replace("# newdoc", "").replace("# newpar", "")
    con_array = conllu.split("\n")
    parsedData = []
    pd = {}
    sentence_id = "# sent_id = "
    article_text = "# text = "
    parseTree = ""
    for line in con_array:
        if len(line) > 0:
            if line.find(sentence_id) != -1:
                if len(parseTree) != 0:
                    pd['parseTree'] = parseTree[:-2]
                    parsedData.append(pd)
                parseTree = ""
                pd = {}
                pd['sent_id'] = int(line[len(sentence_id)-1:].strip())
            elif line.find(article_text) != -1:
                pd['text'] = line[len(article_text)-1:].strip()
            else:
                parseTree += line+"\n"

    if len(parseTree) != 0:
        pd['parseTree'] = parseTree[:-2]
        parsedData.append(pd)
    dat = {
        'url': url,
        'rawText': text,
        'parsedData': parsedData
    }
    db.insert_one(dat)
    global total_processed
    total_processed = total_processed + 1
    print("total processed articles", total_processed)
    print("Parse Tree of the article from", url, "added to MongoDB\n\n")


def process(rdd):
    if not (rdd.isEmpty()):
        df = spark.createDataFrame(rdd, ["url", "raw_text"])
        url = df.select("url").head()
        url = str(url).replace("Row(url='", "")[:-2]
        text = df.select("raw_text").head()
        text = str(text).replace("Row(raw_text='", "")[:-2]
        udpipe_process(url, text)



if __name__ == "__main__":
    sc = SparkContext(appName="Spanish News Stream")
    sc.setLogLevel("ERROR")
    spark = SparkSession.builder.getOrCreate()
    ssc = StreamingContext(sc, 1)
    kvs = KafkaUtils.createDirectStream(
        ssc,
        topics=['espanol-news'],
        kafkaParams={"metadata.broker.list": 'localhost:9092'}
    )
    kvs.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopGraceFully=True)
