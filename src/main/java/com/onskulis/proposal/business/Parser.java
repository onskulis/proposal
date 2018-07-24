package com.onskulis.proposal.business;

import com.onskulis.proposal.model.User;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * Parser class which uses OpenCSV to parse csv data to POJOs
 * and send generated list of users to inbound channel for
 * database insertion
 */
public final class Parser {

    /**
     * Converts CSV Files to POJOs
     * @param filePath path the file
     * @throws Exception if file could not be found
     */
    public void parseFilesToPOJOs(String filePath) throws Exception{

        Reader reader;
        try {
            reader = Files.newBufferedReader(Paths.get(filePath));
        } catch (IOException e) {
            e.printStackTrace();
            throw new Exception("File does not exist");
        }
        CsvToBean<User> csvToBean = new CsvToBeanBuilder(reader)
                    .withType(User.class)
                    .withIgnoreLeadingWhiteSpace(true)
                    .build();

            Iterator<User> csvUserIterator = csvToBean.iterator();

            while (csvUserIterator.hasNext()) {
                User user = csvUserIterator.next();
                //TODO: generate and return a list of users via pipeline for database insertion
            }
    }
}
