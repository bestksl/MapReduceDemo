package com.mapreduceremind.topN.task2;

/**
 * @author HaoxuanLi  Github:bestksl
 * @version created date：2019-09-05 12:54
 */
public class PageCount {

    public void set(String page, int count) {
        this.page = page;
        this.count = count;

    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    private String page;
    private int count;

}
