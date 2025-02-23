package ru.nms.diplom.luceneir;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;

import static ru.nms.diplom.luceneir.utils.Constants.INDEX_DIR;

public class IndexBuilder {
    public static void main(String[] args) throws IOException {
        Directory memoryIndex = FSDirectory.open(Paths.get(INDEX_DIR));
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(memoryIndex, indexWriterConfig);
        readAndIndexDataset(writer);
        writer.close();
    }

    private static void readAndIndexDataset(IndexWriter writer) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader("D:\\diplom\\data\\all_passages.csv"))) {
            String line;
            int i = 0;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] args = line.split("\\^");
                Document document = new Document();
                document.add(new StringField("id", args[1], Field.Store.YES));
                document.add(new TextField("contents", args[0].replaceAll("\"", ""), Field.Store.YES));
                writer.addDocument(document);
            }
        }
    }
}
