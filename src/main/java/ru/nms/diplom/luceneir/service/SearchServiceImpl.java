package ru.nms.diplom.luceneir.service;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;

import static org.apache.lucene.index.IndexWriter.MAX_DOCS;
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
            responseBuilder.addDocuments(ru.nms.diplom.luceneir.service.Document.newBuilder().setId(Integer.parseInt(doc.get("id"))).setScore(scoreDoc.score));
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

        System.out.println("got similarity scores request for ids: " + request.getDocIdList());
        for (Integer id : request.getDocIdList()) {
            TermQuery idQuery = new TermQuery(new Term("id", String.valueOf(id)));


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
            } else {
                System.out.println("did not find similarity score for doc with id " + id);
                responseBuilder.addDocuments(ru.nms.diplom.luceneir.service.Document.newBuilder().setId(id).setScore(0));

            }
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void changeSimilarityParams(BM25TuneRequest request, StreamObserver<Empty> responseObserver) {
        searcher.setSimilarity(new BM25Similarity(request.getK1(), request.getB()));

        System.out.println("set new similarity: " + searcher.getSimilarity());
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void getMinScore(MinScoreRequest request, StreamObserver<MinScoreResponse> responseObserver) {
        Query query;
        try {
            query = parser.parse(QueryParser.escape(request.getQuery()));
        } catch (ParseException e) {
            throw new RuntimeException("Failed to parse query", e);
        }

        TopDocs allDocs;
        try {
            // Retrieve all documents for the query
            allDocs = searcher.search(query, MAX_DOCS);
        } catch (IOException e) {
            throw new RuntimeException("Failed to search documents", e);
        }

        if (allDocs.scoreDocs.length == 0) {
            throw new RuntimeException("No matching documents found.");
        }

        // Find the least similar document (lowest score)
        ScoreDoc leastSimilarDoc = allDocs.scoreDocs[allDocs.scoreDocs.length - 1];
        System.out.println("Least similar document ID: " + leastSimilarDoc.doc + ", Score: " + leastSimilarDoc.score);

        responseObserver.onNext(MinScoreResponse.newBuilder().setScore(leastSimilarDoc.score).build());
        responseObserver.onCompleted();
    }

}
