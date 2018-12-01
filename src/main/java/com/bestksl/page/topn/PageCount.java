package com.bestksl.page.topn;

public class PageCount {
    private int count;
    private String page;

    public void setPageCount(String page, int count) {
        this.page = page;
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }


}
