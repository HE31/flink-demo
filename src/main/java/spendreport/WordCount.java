package spendreport;


/**
 * Created by HE31 on 2020/11/29 18:35
 */
public  class WordCount {
    private String word;
    private Integer count;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public WordCount(String word, Integer count) {
        this.word = word;
        this.count = count;
    }

    public WordCount() {
    }

    @Override
    public String toString() {
        return "WordCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }

}