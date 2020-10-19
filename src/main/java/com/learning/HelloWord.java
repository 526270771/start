package com.learning;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class HelloWord {

    public static void main(String[] args) throws Exception {
        ParameterTool.fromArgs(args);

        //获取flink的运行环境
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String>  dataStream=env.socketTextStream("node01", 9999);
        SingleOutputStreamOperator<Word2Count> word2CountStreamOperator = dataStream.flatMap(new FlatMapFunction<String, Word2Count>() {
            Word2Count word2Count = new Word2Count();

            public void flatMap(String s, Collector<Word2Count> collector) throws Exception {
                for (String s1 : s.split("\\s")) {
                    word2Count.setCount(1);
                    word2Count.setWord(s1);
                }
                collector.collect(word2Count);
            }
        });

        WindowedStream<Word2Count, Tuple, TimeWindow> word = word2CountStreamOperator.keyBy("word").timeWindow(Time.seconds(2), Time.seconds(1));//指定窗口的时间，指定窗口间隔
        word.sum("count").print();
//        SingleOutputStreamOperator<Word2Count> reduce = word.reduce(new RichReduceFunction<Word2Count>() {
//            Word2Count word2Count = new Word2Count();
//            @Override
//            public Word2Count reduce(Word2Count t, Word2Count t1) throws Exception {
//                int count = t.getCount() + t1.getCount();
//                word2Count.setWord(t.getWord());
//                word2Count.setCount(count);
//                return word2Count;
//            }
//        });

        env.execute("helloword");
    }

    public  static  class  Word2Count implements Serializable {
        /*
        *ctrl+shift+/  多行注释
        *
        *
        *
        *
        *
        *
        *
        *
        *
        * */

        private String word;
        private int count;

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }


        @Override
        public String toString() {
            return "Word2Count{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }


}
