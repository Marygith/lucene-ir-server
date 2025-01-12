package ru.nms.diplom.luceneir.service;

import io.grpc.stub.StreamObserver;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;

import static ru.nms.diplom.luceneir.utils.Constants.INDEX_DIR;

public class SearchServiceImpl extends SearchServiceGrpc.SearchServiceImplBase {

    IndexReader reader;
    IndexSearcher searcher;
    StandardAnalyzer analyzer;
    QueryParser parser;

    public SearchServiceImpl() {
        try {
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(INDEX_DIR)));
        } catch (IOException e) {
            throw new RuntimeException("did not manage to open fsd dir with index", e);
        }
        searcher = new IndexSearcher(reader);

        analyzer = new StandardAnalyzer();
        parser = new QueryParser("contents", analyzer);
    }
    @Override
    public void knn(SearchRequest request, StreamObserver<SearchResponse> responseObserver) {

        Query query = null;
        try {
            query = parser.parse(QueryParser.escape(request.getQuery()));
        } catch (ParseException e) {
            throw new RuntimeException("did not manage to parse query", e);
        }

        TopDocs topDocs = null;
        try {
            topDocs = searcher.search(query, request.getK());
        } catch (IOException e) {
            throw new RuntimeException("did not manage to find top docs", e);
        }

        System.out.println("Top " + request.getK() + " search results for query: " + request.getQuery());

        SearchResponse.Builder responseBuilder = SearchResponse.newBuilder();
        float minScore = Arrays.stream(topDocs.scoreDocs).min(Comparator.comparingDouble(sc -> sc.score)).get().score;
        float maxScore = Arrays.stream(topDocs.scoreDocs).max(Comparator.comparingDouble(sc -> sc.score)).get().score;
        float denominator = maxScore == minScore
                ? maxScore
                : (maxScore - minScore);
        if (denominator == 0) throw new RuntimeException("wow denominator is 0, max score was: "+ maxScore + ", min score was: " + minScore);

        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            int docId = scoreDoc.doc;
            Document doc = null;
            try {
                doc = searcher.doc(docId);
            } catch (IOException e) {
                throw new RuntimeException("did not manage to get doc from searcher", e);
            }
            responseBuilder.addDocuments(ru.nms.diplom.luceneir.service.Document.newBuilder().setId(doc.get("id")).setContent(doc.get("contents")).setScore((scoreDoc.score - minScore) / denominator));
            System.out.println("Document ID: " + doc.get("id") + ", Score: " + (scoreDoc.score - minScore) / denominator + ", Contents: " + doc.get("contents"));
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}
