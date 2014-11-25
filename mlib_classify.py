import sys
from operator import add

from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.util import MLUtils

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: trainingset <file>"
        exit(-1)
    sc = SparkContext(appName="PythonMlibClassify")
    
    # Load data in LibSVM format.
    examples = MLUtils.loadLibSVMFile(sc, sys.argv[1])

    # Train a naive Bayes model.
    model = NaiveBayes.train(examples, 1.0)

    # Make predictions
    print "24,30,1 -> ", model.predict([24, 30])
    print "30,40,1 -> ", model.predict([30, 40])
    print "22,49,0 -> ", model.predict([22, 49])
    print "43,39,1 -> ", model.predict([43, 39])
    print "23,30,1 -> ", model.predict([43, 39])

    sc.stop()
