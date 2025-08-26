package com.jd.vearch.model;

public class Hnsw {
    private int nlinks = 32;

    private int efConstruction = 128;

    private int efSearch = 64;

    public void setNlinks(int nlinks){
        this.nlinks = nlinks;
    }

    public int getNlinks(){
        return this.nlinks;
    }

    public void setEfConstruction(int efConstruction){
        this.efConstruction = efConstruction;
    }

    public int getEfConstruction(int efConstruction){
        return this.efConstruction;
    }

    public void setEfSearch(int efSearch){
        this.efConstruction = efSearch;
    }

    public int getEfSearch(){
        return this.efSearch;
    }

}
