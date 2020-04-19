package com.marklogic.kafka.connect.sink;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.*;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.client.io.marker.DocumentMetadataWriteHandle;

import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Using this to facilitate testing. Will likely bring in a real mock-testing library later, like mockito.
 */
public class MockWriteBatcher implements WriteBatcher {

	public String jobId;

	@Override
	public WriteBatcher withDefaultMetadata(DocumentMetadataHandle handle) {
		return null;
	}

	@Override
	public void addAll(Stream<? extends DocumentWriteOperation> operations) {

	}

	@Override
	public DocumentMetadataHandle getDocumentMetadata() {
		return null;
	}

	@Override
	public WriteBatcher add(String uri, AbstractWriteHandle contentHandle) {
		return null;
	}

	@Override
	public WriteBatcher addAs(String uri, Object content) {
		return null;
	}

	@Override
	public WriteBatcher add(String uri, DocumentMetadataWriteHandle metadataHandle, AbstractWriteHandle contentHandle) {
		return null;
	}

	@Override
	public WriteBatcher addAs(String uri, DocumentMetadataWriteHandle metadataHandle, Object content) {
		return null;
	}

	@Override
	public WriteBatcher add(WriteEvent... docs) {
		return null;
	}

	@Override
	public WriteBatcher add(DocumentWriteOperation writeOperation) {
		return null;
	}

	@Override
	public WriteBatcher onBatchSuccess(WriteBatchListener listener) {
		return null;
	}

	@Override
	public WriteBatcher onBatchFailure(WriteFailureListener listener) {
		return null;
	}

	@Override
	public void retry(WriteBatch queryEvent) {

	}

	@Override
	public WriteBatchListener[] getBatchSuccessListeners() {
		return new WriteBatchListener[0];
	}

	@Override
	public WriteFailureListener[] getBatchFailureListeners() {
		return new WriteFailureListener[0];
	}

	@Override
	public void setBatchSuccessListeners(WriteBatchListener... listeners) {

	}

	@Override
	public void setBatchFailureListeners(WriteFailureListener... listeners) {

	}

	@Override
	public WriteBatcher withTemporalCollection(String collection) {
		return null;
	}

	@Override
	public String getTemporalCollection() {
		return null;
	}

	@Override
	public WriteBatcher withTransform(ServerTransform transform) {
		return null;
	}

	@Override
	public ServerTransform getTransform() {
		return null;
	}

	@Override
	public WriteBatcher withForestConfig(ForestConfiguration forestConfig) {
		return null;
	}

	@Override
	public WriteBatcher withJobName(String jobName) {
		return null;
	}

	@Override
	public WriteBatcher withJobId(String jobId) {
		return null;
	}

	@Override
	public WriteBatcher withBatchSize(int batchSize) {
		return null;
	}

	@Override
	public WriteBatcher withThreadCount(int threadCount) {
		return null;
	}

	@Override
	public void flushAsync() {

	}

	@Override
	public void flushAndWait() {

	}

	@Override
	public boolean awaitCompletion() {
		return false;
	}

	@Override
	public boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
		return false;
	}

	@Override
	public JobTicket getJobTicket() {
		return null;
	}

	@Override
	public void retryWithFailureListeners(WriteBatch writeBatch) {

	}

	@Override
	public String getJobName() {
		return null;
	}

	@Override
	public String getJobId() {
		return jobId;
	}

	@Override
	public int getBatchSize() {
		return 0;
	}

	@Override
	public int getThreadCount() {
		return 0;
	}

	@Override
	public ForestConfiguration getForestConfig() {
		return null;
	}

	@Override
	public boolean isStarted() {
		return false;
	}

	@Override
	public boolean isStopped() {
		return false;
	}

	@Override
	public Calendar getJobStartTime() {
		return null;
	}

	@Override
	public Calendar getJobEndTime() {
		return null;
	}

	@Override
	public DatabaseClient getPrimaryClient() {
		return null;
	}
}
