package com.cobub.lucene.test;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.Test;

import java.io.*;
import java.util.Date;

/**
 * Created by feng.wei on 2016/1/13.
 */
public class LuceneOneTest {


    public Document makeDocument(File f) throws FileNotFoundException {
        Document doc = new Document();
        doc.add(new Field("path", f.getPath(), Field.Store.YES, Field.Index.ANALYZED));

        doc.add(new Field("modified"
                , DateTools.timeToString(f.lastModified(), DateTools.Resolution.MINUTE)
                , Field.Store.YES
                , Field.Index.NOT_ANALYZED));

        // Reader implies Store.NO and Index.TOKENIZED
        doc.add(new Field("contents", new FileReader(f)));

        return doc;
    }

    public void test() throws IOException {
        Directory dir = FSDirectory.open(new File("").toPath());
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        IndexWriter indexWriter = new IndexWriter(dir, config);

    }

    @Test
    public void test_file_indexer() throws IOException {
        // fileDir is the directory that contains the text files to be indexed
        File   fileDir  = new File("D:\\test\\org");

        // indexDir is the directory that hosts Lucene's index files
        Directory indexDir = FSDirectory.open(new File("D:\\test\\index").toPath());
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        IndexWriter indexWriter = new IndexWriter(indexDir,config);
        File[] textFiles  = fileDir.listFiles();
        long startTime = new Date().getTime();

        //Add documents to the index
        for(int i = 0; i < textFiles.length; i++){
            if(textFiles[i].isFile() && textFiles[i].getName().endsWith(".txt")){
                System.out.println("File " + textFiles[i].getCanonicalPath()
                        + " is being indexed");
                Reader textReader = new FileReader(textFiles[i]);
                Document document = new Document();
                document.add(new Field("content", textReader));
                document.add(new Field("path", textFiles[i].getPath(),Field.Store.YES, Field.Index.ANALYZED));
                indexWriter.addDocument(document);
            }
        }

        // indexWriter.commit();
        indexWriter.close();
        long endTime = new Date().getTime();

        System.out.println("It took " + (endTime - startTime)
                + " milliseconds to create an index for the files in the directory "
                + fileDir.getPath());
    }

    @Test
    public void test_files_search() throws IOException {
        String queryStr = "Earth";
        //This is the directory that hosts the Lucene index
        File indexDir = new File("D:\\test\\index");
        FSDirectory directory = FSDirectory.open(indexDir.toPath());

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        if(!indexDir.exists()){
            System.out.println("The Lucene index is not exist");
            return;
        }
        Term term = new Term("content",queryStr.toLowerCase());
        TermQuery luceneQuery = new TermQuery(term);
        TopDocs docs = searcher.search(luceneQuery, 10);
        ScoreDoc[] hits = docs.scoreDocs;
        for(int i = 0; i < hits.length; i++){
            ScoreDoc doc = hits[i];
            System.out.println("doc=" + doc.doc + ", score=" + doc.score);
        }
    }

}
