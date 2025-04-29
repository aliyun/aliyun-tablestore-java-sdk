package com.alicloud.openservices.tablestore.tunnel.pipeline;

import com.alicloud.openservices.tablestore.model.StreamRecord;
import com.alicloud.openservices.tablestore.model.tunnel.internal.CheckpointResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ReadRecordsRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ReadRecordsResponse;
import com.alicloud.openservices.tablestore.tunnel.worker.ChannelConnect;
import com.alicloud.openservices.tablestore.tunnel.worker.ChannelConnectStatus;
import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.ProcessRecordsInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;

import static com.alicloud.openservices.tablestore.core.protocol.ResponseFactory.FINISH_TAG;

/**
 * ProcessDataPipeline is a cyclic pipeline task on ChannelConnect for executing data reading and data processing. 
 * Each stage can specify its particular execution resources (thread pool). Each stage of the task will only occupy the processing resources of the corresponding stage, without direct interference between stages. The relevant thread pool configurations are in TunnelWorkerConfig (readRecordsExecutor && processRecordsExecutor).
 * When each round of data reading and data processing is completed without any errors, a new processing cycle will start immediately.
 */
public class ProcessDataPipeline implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessDataPipeline.class);

    private final ChannelConnect connect;
    private volatile boolean started = false;
    private final ThreadPoolExecutor readRecordsExecutor;
    private final ThreadPoolExecutor processRecordsExecutor;
    private Semaphore semaphore;
    private int readMaxTimesPerRound = 1;
    private int readMaxBytesPerRound = 4 * 1024 * 1024; // 4M bytes

    /**
     * pipelineHelperExecutor is used for pipeline initialization, error handling during runtime, etc.
     */
    private final ExecutorService pipelineHelperExecutor;

    private ProcessDataBackoff backoff;
    private Pipeline<ReadRecordsRequest, CheckpointResponse> pipeline;

    private static final int COUNT_BAR = 500;
    private static final int SIZE_BAR = 900 * 1024; // 900K bytes

    public ProcessDataPipeline(ChannelConnect connect, ExecutorService helperExecutor,
                               ThreadPoolExecutor readRecordsExecutor, ThreadPoolExecutor processRecordsExecutor) {
        this.connect = connect;
        this.pipelineHelperExecutor = helperExecutor;
        this.readRecordsExecutor = readRecordsExecutor;
        this.processRecordsExecutor = processRecordsExecutor;
    }

    public ProcessDataPipeline(ChannelConnect connect, ExecutorService helperExecutor,
                               ThreadPoolExecutor readRecordsExecutor, ThreadPoolExecutor processRecordsExecutor,
                               Semaphore semaphore) {
        this.connect = connect;
        this.pipelineHelperExecutor = helperExecutor;
        this.readRecordsExecutor = readRecordsExecutor;
        this.processRecordsExecutor = processRecordsExecutor;
        this.semaphore = semaphore;
    }

    @Override
    public void run() {
        // If the semaphore exists, check the semaphore first before running.
        if (semaphore != null) {
            try {
                semaphore.acquire();
                LOG.info("Tunnel {}, Channel {} acquire semaphore succeed, permits remaining: {}",
                        connect.getTunnelId(), connect.getChannelId(), semaphore.availablePermits());
                LOG.debug("Semaphore permits remaining {}, queued {}", semaphore.availablePermits(), semaphore.getQueueLength());
            } catch (InterruptedException e) {
                LOG.warn("Acquire semaphore failed: {}, ChannelId: {}", e.toString(), connect.getChannelId());
                semaphore.release();
            }
        }
        // When the Pipeline is run for the first time, initialization operations are performed. The execution of a single Pipeline instance is serial, so there will be no race conditions.
        if (!started) {
            LOG.info("Initial process data pipeline.");
            this.pipeline = buildPipeline();
            pipeline.init(new ProcessDataPipelineContext(connect, semaphore));
            started = true;
        }
        this.pipeline.process(new ReadRecordsRequest(connect.getTunnelId(), connect.getClientId(),
                connect.getChannelId(), connect.getToken()));
    }

    private Pipeline<ReadRecordsRequest, CheckpointResponse> buildPipeline() {
        final Pipeline<ReadRecordsRequest, CheckpointResponse> pipeline =
                new Pipeline<ReadRecordsRequest, CheckpointResponse>(pipelineHelperExecutor);

        // Insert the two stages of the task in sequence.
        Stage<ReadRecordsRequest, ProcessRecordsInput> readRecordsStage = createReadRecordsStage();
        pipeline.addExecutorForStage(readRecordsStage, readRecordsExecutor);

        Stage<ProcessRecordsInput, Boolean> processRecordStage = createProcessRecordsStage();
        pipeline.addExecutorForStage(processRecordStage, processRecordsExecutor);

        return pipeline;
    }

    private Stage<ReadRecordsRequest, ProcessRecordsInput> createReadRecordsStage() {
        return new AbstractStage<ReadRecordsRequest, ProcessRecordsInput>() {
            @Override
            public ProcessRecordsInput doProcess(ReadRecordsRequest readRecordsRequest) throws StageException {
                if (connect.getStatus() == ChannelConnectStatus.RUNNING) {
                    if (connect.getToken() != null && !(FINISH_TAG).equals(connect.getToken())) {
                        try {
                            LOG.debug("Begin read records, connect: {}", connect);
                            long beginTs = System.currentTimeMillis();
                            ReadRecordsResponse resp = null;
                            List<StreamRecord> totalRecords = new LinkedList<StreamRecord>();
                            int totalBytes = 0, times = 0, totalRecordsCount = 0;
                            while (totalBytes < readMaxBytesPerRound && times < readMaxTimesPerRound) {
                                resp = connect.getClient().readRecords(readRecordsRequest);
                                totalRecords.addAll(resp.getRecords());
                                totalRecordsCount += resp.getRecords().size();
                                totalBytes += resp.getMemoizedSerializedSize();
                                times++;

                                if (resp.getNextToken() == null || FINISH_TAG.equals(resp.getNextToken())) {
                                    LOG.info("Channel {} next token is null", connect.getChannelId());
                                    break;
                                }
                                if (backoff != null) {
                                    if (needResetBackoff(resp.getRecords().size(), resp.getMemoizedSerializedSize(), resp.getMayMoreRecord())) {
                                        LOG.debug("Backoff is reset");
                                        backoff.reset();
                                    } else {
                                        long sleepMills = backoff.nextBackOffMillis();
                                        LOG.debug("Data is not full, sleep {} msec.", sleepMills);
                                        Thread.sleep(backoff.nextBackOffMillis());
                                        break;
                                    }
                                }
                                readRecordsRequest.setToken(resp.getNextToken());
                            }
                            if (resp == null) {
                                LOG.info("ReadRecordsResponse is null, channelId: {}", connect.getChannelId());
                                return new ProcessRecordsInput(totalRecords, null, null);
                            } else {
                                LOG.info("GetRecords, Num: {}, LoopTimes: {}, TotalBytes: {}, Channel connect: {}, Latency: {} ms, Next Token: {}",
                                        totalRecordsCount, times, totalBytes, connect, System.currentTimeMillis() - beginTs, resp.getNextToken());
                                return new ProcessRecordsInput(totalRecords, resp.getNextToken(), resp.getRequestId(), connect.getChannelId());
                            }
                        } catch (Exception e) {
                            throw new StageException(this, readRecordsRequest, e.getMessage(), e);
                        }
                    } else {
                        LOG.info("Channel is finished, channel will be closed.");
                        connect.close(true);
                        throw new StageException(this, readRecordsRequest, "Channel connect is finished.");
                    }
                } else {
                    throw new StageException(this, readRecordsRequest, "Channel is not running.");
                }
            }
        };
    }

    private Stage<ProcessRecordsInput, Boolean> createProcessRecordsStage() {
        return new AbstractStage<ProcessRecordsInput, Boolean>() {
            @Override
            public Boolean doProcess(ProcessRecordsInput processRecordsInput) throws StageException {
                if (connect.getStatus() == ChannelConnectStatus.RUNNING) {
                    try {
                        IChannelProcessor processor = connect.getProcessor();
                        processor.process(processRecordsInput);

                        // everything is ok, set channel connect new token, and loop pipeline.
                        connect.setToken(processRecordsInput.getNextToken());
                        LOG.info("Continue run pipeline, connect: {}", connect);
                        connect.getChannelExecutorService().submit(connect.getProcessPipeline());
                        if (semaphore != null) {
                            semaphore.release();
                            LOG.info("Channel {} release semaphore succeed", connect.getChannelId());
                            LOG.debug("Semaphore permits remaining {}, queued {}", semaphore.availablePermits(), semaphore.getQueueLength());
                        }
                        return true;
                    } catch (Exception e) {
                        throw new StageException(this, processRecordsInput, e.getMessage(), e);
                    }
                } else {
                    throw new StageException(this, processRecordsInput, "Channel is not running.");
                }
            }
        };
    }


    private boolean needResetBackoff(int numRec, int size, Boolean mayMoreRecord) {
        if (mayMoreRecord != null) {
            return mayMoreRecord;
        }
        return checkDataEnough(numRec, size);
    }


    /**
     * Determine if the current batch of data fetched is sufficiently large, used for the data fetching sleep strategy.
     *
     * @param numRec: Number of data records
     * @param size:   Total number of bytes for the records
     * @return true indicates that the data fetched in the current batch is sufficiently large, otherwise, return false indicating the data fetched in the current batch is not enough.
     */
    private boolean checkDataEnough(int numRec, int size) {
        return numRec > COUNT_BAR || size > SIZE_BAR;
    }

    public ProcessDataBackoff getBackoff() {
        return backoff;
    }

    public void setBackoff(ProcessDataBackoff backoff) {
        this.backoff = backoff;
    }

    public Semaphore getSemaphore() {
        return semaphore;
    }

    public void setSemaphore(Semaphore semaphore) {
        this.semaphore = semaphore;
    }

    public void setReadMaxTimesPerRound(int readMaxTimesPerRound) {
        this.readMaxTimesPerRound = readMaxTimesPerRound;
    }

    public void setReadMaxBytesPerRound(int readMaxBytesPerRound) {
        this.readMaxBytesPerRound = readMaxBytesPerRound;
    }
}
