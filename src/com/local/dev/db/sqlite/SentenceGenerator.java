package com.local.dev.db.sqlite;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SentenceGenerator {
    private List<String> dictionary;

    public SentenceGenerator(List<String> dictionary) {
        this.dictionary = dictionary;
    }

    public String generate(int sizeInBytes) {
        String str = "";
        Random rand = new Random();

        while (str.length() <= sizeInBytes) {
            str += this.dictionary.get(rand.nextInt(this.dictionary.size())) + " ";
        }

        return str.trim();
    }

    public List<String> generateSentenceList(int listSize, int sizeInBytes) {
        List<String> list = new ArrayList<String>();

        for (int i = 0; i < listSize; i++) {
            list.add(this.generate(sizeInBytes));
        }

        return list;
    }

}
