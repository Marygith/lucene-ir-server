package ru.nms.diplom.luceneir.service;

import io.grpc.stub.StreamObserver;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;

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
    public void knn(SearchRequest request, StreamObserver<DocumentsResponse> responseObserver) {

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

        DocumentsResponse.Builder responseBuilder = DocumentsResponse.newBuilder();

        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            int docId = scoreDoc.doc;
            Document doc = null;
            try {
                doc = searcher.doc(docId);
            } catch (IOException e) {
                throw new RuntimeException("did not manage to get doc from searcher", e);
            }
            responseBuilder.addDocuments(ru.nms.diplom.luceneir.service.Document.newBuilder().setId(doc.get("id")).setScore(scoreDoc.score));
            System.out.println("Document ID: " + doc.get("id") + ", Score: " + scoreDoc.score + ", Contents: " + doc.get("contents"));
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getSimilarityScores(SimilarityScoreRequest request, StreamObserver<DocumentsResponse> responseObserver) {
        Query contentQuery = null;
        try {
            contentQuery = parser.parse(QueryParser.escape(request.getQuery()));
        } catch (ParseException e) {
            throw new RuntimeException("did not manage to parse query", e);
        }
        DocumentsResponse.Builder responseBuilder = DocumentsResponse.newBuilder();

        for (String id : request.getDocIdList()) {
            TermQuery idQuery = new TermQuery(new Term("id", id));


            BooleanQuery combinedQuery = new BooleanQuery.Builder()
                    .add(contentQuery, BooleanClause.Occur.MUST)
                    .add(idQuery, BooleanClause.Occur.FILTER)
                    .build();

            TopDocs topDocs = null;
            try {
                topDocs = searcher.search(combinedQuery, 1);
            } catch (IOException e) {
                throw new RuntimeException("did not manage to get doc from index", e);
            }

            if (topDocs.totalHits.value > 0) {
                ScoreDoc scoreDoc = topDocs.scoreDocs[0];
                responseBuilder.addDocuments(ru.nms.diplom.luceneir.service.Document.newBuilder().setId(id).setScore(scoreDoc.score));
            }
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}
