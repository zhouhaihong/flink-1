/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * This is the single (non-parallel) monitoring task which takes a {@link FileInputFormat} and,
 * depending on the {@link FileProcessingMode} and the {@link FilePathFilter}, it is responsible
 * for:
 *
 * <ol>
 *   <li>Monitoring a user-provided path. 监控用户提供的路径
 *   <li>Deciding which files should be further read and processed. 决定接下来读取和处理哪个文件
 *   <li>Creating the {@link FileInputSplit splits} corresponding to those files.创建与这些文件对应的FileInputSplit。
 *   <li>Assigning them to downstream tasks for further processing.将Split分配给下游，以便做进一步处理
 * </ol>
 *
 * <p>The splits to be read are forwarded to the downstream {@link ContinuousFileReaderOperator}
 * which can have parallelism greater than one.
 *
 * <p><b>IMPORTANT NOTE: </b> Splits are forwarded downstream for reading in ascending modification
 * time order, based on the modification time of the files they belong to.
 * 转发给下游的Split会按照该split所属的文件的修改时间，进行升序排序，依次下发给下游
 */
@Internal
public class ContinuousFileMonitoringFunction<OUT>
        extends RichSourceFunction<TimestampedFileInputSplit> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(ContinuousFileMonitoringFunction.class);

    /**
     * The minimum interval allowed between consecutive path scans.
     *
     * <p><b>NOTE:</b> Only applicable to the {@code PROCESS_CONTINUOUSLY} mode.
     */
    public static final long MIN_MONITORING_INTERVAL = 1L;

    /** The path to monitor. */
    private final String path;

    /** The parallelism of the downstream readers. 这里的并行度指的是，下游读取问价的并行度 */
    private final int readerParallelism;

    /** The {@link FileInputFormat} to be read. */
    private final FileInputFormat<OUT> format;

    /** The interval between consecutive path scans. */
    private final long interval;

    /** Which new data to process (see {@link FileProcessingMode}. */
    private final FileProcessingMode watchType;

    /** The maximum file modification time seen so far. */
    private volatile long globalModificationTime;

    private transient Object checkpointLock;

    private volatile boolean isRunning = true;

    private transient ListState<Long> checkpointedState;

    public ContinuousFileMonitoringFunction(
            FileInputFormat<OUT> format,
            FileProcessingMode watchType,
            int readerParallelism,
            long interval) {

        Preconditions.checkArgument(
                watchType == FileProcessingMode.PROCESS_ONCE || interval >= MIN_MONITORING_INTERVAL,
                "The specified monitoring interval ("
                        + interval
                        + " ms) is smaller than the minimum "
                        + "allowed one ("
                        + MIN_MONITORING_INTERVAL
                        + " ms).");

        Preconditions.checkArgument(
                format.getFilePaths().length == 1,
                "FileInputFormats with multiple paths are not supported yet.");

        this.format = Preconditions.checkNotNull(format, "Unspecified File Input Format.");
        this.path =
                Preconditions.checkNotNull(
                        format.getFilePaths()[0].toString(), "Unspecified Path.");

        this.interval = interval;
        this.watchType = watchType;
        this.readerParallelism = Math.max(readerParallelism, 1);
        this.globalModificationTime = Long.MIN_VALUE;
    }

    @VisibleForTesting
    public long getGlobalModificationTime() {
        return this.globalModificationTime;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        Preconditions.checkState(
                this.checkpointedState == null,
                "The " + getClass().getSimpleName() + " has already been initialized.");

        this.checkpointedState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "file-monitoring-state", LongSerializer.INSTANCE));

        if (context.isRestored()) {
            LOG.info("Restoring state for the {}.", getClass().getSimpleName());

            List<Long> retrievedStates = new ArrayList<>();
            for (Long entry : this.checkpointedState.get()) {
                retrievedStates.add(entry);
            }

            // given that the parallelism of the function is 1, we can only have 1 or 0 retrieved
            // items.
            // the 0 is for the case that we are migrating from a previous Flink version.

            Preconditions.checkArgument(
                    retrievedStates.size() <= 1,
                    getClass().getSimpleName() + " retrieved invalid state.");

            if (retrievedStates.size() == 1 && globalModificationTime != Long.MIN_VALUE) {
                // this is the case where we have both legacy and new state.
                // The two should be mutually exclusive for the operator, thus we throw the
                // exception.

                throw new IllegalArgumentException(
                        "The "
                                + getClass().getSimpleName()
                                + " has already restored from a previous Flink version.");

            } else if (retrievedStates.size() == 1) {
                this.globalModificationTime = retrievedStates.get(0);
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "{} retrieved a global mod time of {}.",
                            getClass().getSimpleName(),
                            globalModificationTime);
                }
            }

        } else {
            LOG.info("No state to restore for the {}.", getClass().getSimpleName());
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        format.configure(parameters);

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Opened {} (taskIdx= {}) for path: {}",
                    getClass().getSimpleName(),
                    getRuntimeContext().getIndexOfThisSubtask(),
                    path);
        }
    }

    @Override
    public void run(SourceFunction.SourceContext<TimestampedFileInputSplit> context)
            throws Exception {
        Path p = new Path(path);
        FileSystem fileSystem = FileSystem.get(p.toUri());
        if (!fileSystem.exists(p)) {
            throw new FileNotFoundException("The provided file path " + path + " does not exist.");
        }

        checkpointLock = context.getCheckpointLock();
        switch (watchType) {
            case PROCESS_CONTINUOUSLY:
                while (isRunning) {
                    synchronized (checkpointLock) {
                        monitorDirAndForwardSplits(fileSystem, context);
                    }
                    Thread.sleep(interval);
                }

                // here we do not need to set the running to false and the
                // globalModificationTime to Long.MAX_VALUE because to arrive here,
                // either close() or cancel() have already been called, so this
                // is already done.

                break;
            case PROCESS_ONCE:
                synchronized (checkpointLock) {

                    // the following check guarantees that if we restart
                    // after a failure and we managed to have a successful
                    // checkpoint, we will not reprocess the directory.

                    if (globalModificationTime == Long.MIN_VALUE) {
                        monitorDirAndForwardSplits(fileSystem, context);
                        globalModificationTime = Long.MAX_VALUE;
                    }
                    isRunning = false;
                }
                break;
            default:
                isRunning = false;
                throw new RuntimeException("Unknown WatchType" + watchType);
        }
    }

    private void monitorDirAndForwardSplits(
            FileSystem fs, SourceContext<TimestampedFileInputSplit> context) throws IOException {
        assert (Thread.holdsLock(checkpointLock));

        // 获取需要处理的文件
        Map<Path, FileStatus> eligibleFiles = listEligibleFiles(fs, new Path(path));

        // 获取目录下，当前所有未处理文件的分片。并按照时间进行了排除处理（利用TreeMap实现）
        Map<Long, List<TimestampedFileInputSplit>> splitsSortedByModTime =
                getInputSplitsSortedByModTime(eligibleFiles);

        for (Map.Entry<Long, List<TimestampedFileInputSplit>> splits :
                splitsSortedByModTime.entrySet()) {
            long modificationTime = splits.getKey();
            for (TimestampedFileInputSplit split : splits.getValue()) {
                LOG.info("Forwarding split: " + split);
                // 将Split信息发送给下游算子进行读取数据处理
                context.collect(split);
            }
            // update the global modification time 更新全局修改时间
            globalModificationTime = Math.max(globalModificationTime, modificationTime);
        }
    }

    /**
     * 创建要转发到ContinuousFileReaderOperator的下游任务的InputSplt。
     * Split在转发之前按修改时间排序，并且只处理属于符合条件的文件列表中的文件Split。
     * Creates the input splits to be forwarded to the downstream tasks of the {@link
     * ContinuousFileReaderOperator}. Splits are sorted <b>by modification time</b> before being
     * forwarded and only splits belonging to files in the {@code eligibleFiles} list will be
     * processed.
     *
     * @param eligibleFiles The files to process.
     */
    private Map<Long, List<TimestampedFileInputSplit>> getInputSplitsSortedByModTime(
            Map<Path, FileStatus> eligibleFiles) throws IOException {

        Map<Long, List<TimestampedFileInputSplit>> splitsByModTime = new TreeMap<>();
        if (eligibleFiles.isEmpty()) {
            return splitsByModTime;
        }

        for (FileInputSplit split : format.createInputSplits(readerParallelism)) {
            FileStatus fileStatus = eligibleFiles.get(split.getPath());
            if (fileStatus != null) {
                Long modTime = fileStatus.getModificationTime();
                List<TimestampedFileInputSplit> splitsToForward = splitsByModTime.get(modTime);
                if (splitsToForward == null) {
                    splitsToForward = new ArrayList<>();
                    splitsByModTime.put(modTime, splitsToForward);
                }
                splitsToForward.add(
                        new TimestampedFileInputSplit(
                                modTime,
                                split.getSplitNumber(),
                                split.getPath(),
                                split.getStart(),
                                split.getLength(),
                                split.getHostnames()));
            }
        }
        return splitsByModTime;
    }

    /**
     * Returns the paths of the files not yet processed.
     *
     * @param fileSystem The filesystem where the monitored directory resides.
     */
    private Map<Path, FileStatus> listEligibleFiles(FileSystem fileSystem, Path path) {

        final FileStatus[] statuses;
        try {
            statuses = fileSystem.listStatus(path);
        } catch (IOException e) {
            // we may run into an IOException if files are moved while listing their status
            // delay the check for eligible files in this case
            return Collections.emptyMap();
        }

        if (statuses == null) {
            LOG.warn("Path does not exist: {}", path);
            return Collections.emptyMap();
        } else {
            Map<Path, FileStatus> files = new HashMap<>();
            // handle the new files
            for (FileStatus status : statuses) {
                if (!status.isDir()) {
                    Path filePath = status.getPath();
                    long modificationTime = status.getModificationTime();
                    // 判断文件是否是处理过的文件，如果处理过，则忽略
                    if (!shouldIgnore(filePath, modificationTime)) {
                        files.put(filePath, status);
                    }
                } else if (format.getNestedFileEnumeration() && format.acceptFile(status)) { //判断目录是否允许递归遍历 & 该文件不是.和_开头
                    files.putAll(listEligibleFiles(fileSystem, status.getPath()));
                }
            }
            return files;
        }
    }

    /**
     * 如果该文件不需要处理，则返回TRUE。
     * 如果文件的修改时间小于globalModificationTime，则会出现这种情况。
     * Returns {@code true} if the file is NOT to be processed further. This happens if the
     * modification time of the file is smaller than the {@link #globalModificationTime}.
     *
     * @param filePath the path of the file to check.
     * @param modificationTime the modification time of the file.
     */
    private boolean shouldIgnore(Path filePath, long modificationTime) {
        assert (Thread.holdsLock(checkpointLock));
        boolean shouldIgnore = modificationTime <= globalModificationTime;
        if (shouldIgnore && LOG.isDebugEnabled()) {
            LOG.debug(
                    "Ignoring "
                            + filePath
                            + ", with mod time= "
                            + modificationTime
                            + " and global mod time= "
                            + globalModificationTime);
        }
        return shouldIgnore;
    }

    @Override
    public void close() throws Exception {
        super.close();

        if (checkpointLock != null) {
            synchronized (checkpointLock) {
                globalModificationTime = Long.MAX_VALUE;
                isRunning = false;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Closed File Monitoring Source for path: " + path + ".");
        }
    }

    @Override
    public void cancel() {
        if (checkpointLock != null) {
            // this is to cover the case where cancel() is called before the run()
            synchronized (checkpointLock) {
                globalModificationTime = Long.MAX_VALUE;
                isRunning = false;
            }
        } else {
            globalModificationTime = Long.MAX_VALUE;
            isRunning = false;
        }
    }

    //	---------------------			Checkpointing			--------------------------

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        Preconditions.checkState(
                this.checkpointedState != null,
                "The " + getClass().getSimpleName() + " state has not been properly initialized.");

        this.checkpointedState.clear();
        this.checkpointedState.add(this.globalModificationTime);

        if (LOG.isDebugEnabled()) {
            LOG.debug("{} checkpointed {}.", getClass().getSimpleName(), globalModificationTime);
        }
    }
}
