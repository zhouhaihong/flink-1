/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.Serializable;

/**
 * SourceFunction是所有流数据源的基本结构. 当source开始发射element时, SouceFunction的run方法会被调用,以用
 * 于发射element, run方法会一直运行,cancel必须提供退出循环的方式
 * Base interface for all stream data sources in Flink. The contract of a stream source is the
 * following: When the source should start emitting elements, the {@link #run} method is called with
 * a {@link SourceContext} that can be used for emitting elements. The run method can run for as
 * long as necessary. The source must, however, react to an invocation of {@link #cancel()} by
 * breaking out of its main loop.
 *
 * <h3>CheckpointedFunction Sources</h3>
 *
 * Source如果实现了CheckpointedFunction接口,那么必须确保不能同时执行state checkpoint、算子内部状态更新和算子发射元
 * 素(即这两个操作之间不存在并发问题). 具体的解决方案是在synchronized同步代码块中使用flink中提供的checkpoint lock对
 * 象来保护对于状态的更新、发射element.
 * 特别说明：initializeState和snapshotState方法中不用加同步操作,同步操作由flink内部进行控制.
 * <p>Sources that also implement the {@link
 * org.apache.flink.streaming.api.checkpoint.CheckpointedFunction} interface must ensure that state
 * checkpointing, updating of internal state and emission of elements are not done concurrently.
 * This is achieved by using the provided checkpointing lock object to protect update of state and
 * emission of elements in a synchronized block.
 *
 *具体的使用方式,可以参考下面的编程范式来做.
 * <p>This is the basic pattern one should follow when implementing a checkpointed source:
 *
 * <pre>{@code
 *  public class ExampleCountSource implements SourceFunction<Long>, CheckpointedFunction {
 *      private long count = 0L;
 *      private volatile boolean isRunning = true;
 *
 *      private transient ListState<Long> checkpointedCount;
 *
 *      public void run(SourceContext<T> ctx) {
 *          while (isRunning && count < 1000) {
 *              // this synchronized block ensures that state checkpointing,
 *              // internal state updates and emission of elements are an atomic operation
 *              synchronized (ctx.getCheckpointLock()) {
 *                  ctx.collect(count);
 *                  count++;
 *              }
 *          }
 *      }
 *
 *      public void cancel() {
 *          isRunning = false;
 *      }
 *
 *      public void initializeState(FunctionInitializationContext context) {
 *          this.checkpointedCount = context
 *              .getOperatorStateStore()
 *              .getListState(new ListStateDescriptor<>("count", Long.class));
 *
 *          if (context.isRestored()) {
 *              for (Long count : this.checkpointedCount.get()) {
 *                  this.count = count;
 *              }
 *          }
 *      }
 *
 *      public void snapshotState(FunctionSnapshotContext context) {
 *          this.checkpointedCount.clear();
 *          this.checkpointedCount.add(count);
 *      }
 * }
 * }</pre>
 *
 * <h3>Timestamps and watermarks:</h3>
 *
 * <p>Sources may assign timestamps to elements and may manually emit watermarks. However, these are
 * only interpreted if the streaming program runs on {@link TimeCharacteristic#EventTime}. On other
 * time characteristics ({@link TimeCharacteristic#IngestionTime} and {@link
 * TimeCharacteristic#ProcessingTime}), the watermarks from the source function are ignored.
 *
 * @param <T> The type of the elements produced by this source.
 * @see org.apache.flink.streaming.api.TimeCharacteristic
 */
@Public
public interface SourceFunction<T> extends Function, Serializable {

    /**
     * 启动Source.实现类可以使用SourceContext中提供的相应方法发射element
     * Starts the source. Implementations can use the {@link SourceContext} emit elements.
     *
     * 如果Source实现了CheckpointedFunction,在更新内部状态和发射element时，必须使用checkpoint lock进行
     * 加锁处理（可以使用synchronized代码块加锁），以保证在做状态的检查点与内部状态更新和发射element之间的原子性
     *
     * <p>Sources that implement {@link
     * org.apache.flink.streaming.api.checkpoint.CheckpointedFunction} must lock on the checkpoint
     * lock (using a synchronized block) before updating internal state and emitting elements, to
     * make both an atomic operation:
     *
     * <pre>{@code
     *  public class ExampleCountSource implements SourceFunction<Long>, CheckpointedFunction {
     *      private long count = 0L;
     *      private volatile boolean isRunning = true;
     *
     *      private transient ListState<Long> checkpointedCount;
     *
     *      public void run(SourceContext<T> ctx) {
     *          while (isRunning && count < 1000) {
     *              // this synchronized block ensures that state checkpointing,
     *              // internal state updates and emission of elements are an atomic operation
     *              synchronized (ctx.getCheckpointLock()) {
     *                  ctx.collect(count);
     *                  count++;
     *              }
     *          }
     *      }
     *
     *      public void cancel() {
     *          isRunning = false;
     *      }
     *
     *      public void initializeState(FunctionInitializationContext context) {
     *          this.checkpointedCount = context
     *              .getOperatorStateStore()
     *              .getListState(new ListStateDescriptor<>("count", Long.class));
     *
     *          if (context.isRestored()) {
     *              for (Long count : this.checkpointedCount.get()) {
     *                  this.count = count;
     *              }
     *          }
     *      }
     *
     *      public void snapshotState(FunctionSnapshotContext context) {
     *          this.checkpointedCount.clear();
     *          this.checkpointedCount.add(count);
     *      }
     * }
     * }</pre>
     *
     * @param ctx The context to emit elements to and for accessing locks.
     */
    void run(SourceContext<T> ctx) throws Exception;

    /**
     * 取消Source. 大多数的Source在run方法的内部都是用了while循环. 该方法在调用时,必须保证Source能够退出循环
     * Cancels the source. Most sources will have a while loop inside the {@link
     * #run(SourceContext)} method. The implementation needs to ensure that the source will break
     * out of that loop after this method is called.
     *
     * 通常的做法是定义一个volatile boolean isRunning标识，以检查是否应该退出循环
     * <p>A typical pattern is to have an {@code "volatile boolean isRunning"} flag that is set to
     * {@code false} in this method. That flag is checked in the loop condition.
     *
     * 当Source被取消时，执行线程会通过Thread.interrupt方法被中断。中断严格发生在调用此方法之后，因此
     * 任何中断处理程序都可以依赖于此方法已完成这一事实。较好的实践是，将通过此方法更改的任何标志设置为“易失性”，以保证
     * 此方法对任何中断处理程序的影响可见性。
     * <p>When a source is canceled, the executing thread will also be interrupted (via {@link
     * Thread#interrupt()}). The interruption happens strictly after this method has been called, so
     * any interruption handler can rely on the fact that this method has completed. It is good
     * practice to make any flags altered by this method "volatile", in order to guarantee the
     * visibility of the effects of this method to any interruption handler.
     */
    void cancel();

    // ------------------------------------------------------------------------
    //  source context
    // ------------------------------------------------------------------------

    /**
     * 该接口定义了SourceFunction发送element和watermark的相关方法
     * Interface that source functions use to emit elements, and possibly watermarks.
     *
     * @param <T> The type of the elements produced by the source.
     */
    @Public // Interface might be extended in the future with additional methods.
    interface SourceContext<T> {

        /**
         * 从source端发射一个element，无timestamp，在大多数场景下，这是默认的发射element的方法。
         * Emits one element from the source, without attaching a timestamp. In most cases, this is
         * the default way of emitting elements.
         * 该方法element的timestamp取决于具体的时间语义。
         * <p>The timestamp that the element will get assigned depends on the time characteristic of
         * the streaming program:
         *
         * <ul>
         *      时间语义为ProcessingTime，element无timestamp
         *   <li>On {@link TimeCharacteristic#ProcessingTime}, the element has no timestamp.
         *      时间语义为IngestionTime，element的timestamp为当前系统时间
         *   <li>On {@link TimeCharacteristic#IngestionTime}, the element gets the system's current
         *       time as the timestamp.
         *      时间语义为EventTime,该element最初将没有timestamp。在任何与时间相关的操作(如时间窗口)之前，它需要通过
         *      TimestampAssigner获取时间戳
         *   <li>On {@link TimeCharacteristic#EventTime}, the element will have no timestamp
         *       initially. It needs to get a timestamp (via a {@link TimestampAssigner}) before any
         *       time-dependent operation (like time windows).
         *          * </ul>
         *
         * @param element The element to emit
         */
        void collect(T element);

        /**
         * 从source端发射一个element，并给该element附带上一个timestamp，该方法与程序使用EventTime时间语义相关，
         * 即其timeStamp是由Source端自行分配，而不是依赖于Stream上的TimestampAssigner。
         * Emits one element from the source, and attaches the given timestamp. This method is
         * relevant for programs using {@link TimeCharacteristic#EventTime}, where the sources
         * assign timestamps themselves, rather than relying on a {@link TimestampAssigner} on the
         * stream.
         *
         * <p>On certain time characteristics, this timestamp may be ignored or overwritten. This
         * allows programs to switch between the different time characteristics and behaviors
         * without changing the code of the source functions.
         *
         * <ul>如果时间语义为ProcessingTime，指定的时间搓会被忽略
         *   <li>On {@link TimeCharacteristic#ProcessingTime}, the timestamp will be ignored,
         *       because processing time never works with element timestamps.
         *      如果时间语义为IngestionTime，指定的timestamp会被source端当前系统的时间戳覆盖
         *   <li>On {@link TimeCharacteristic#IngestionTime}, the timestamp is overwritten with the
         *       system's current time, to realize proper ingestion time semantics.
         *      如果时间语义为EventTime，则生效
         *   <li>On {@link TimeCharacteristic#EventTime}, the timestamp will be used.
         * </ul>
         *
         * @param element The element to emit
         * @param timestamp The timestamp in milliseconds since the Epoch
         */
        @PublicEvolving
        void collectWithTimestamp(T element, long timestamp);

        /**
         * 发射给定的watermark,如果watermark的值为t,那么说明不会再有<=t的element来了,如果有这样的element出现,
         * 那么这些element将作为迟到数据
         * Emits the given {@link Watermark}. A Watermark of value {@code t} declares that no
         * elements with a timestamp {@code t' <= t} will occur any more. If further such elements
         * will be emitted, those elements are considered <i>late</i>.
         *
         * 该方法只在时间语义为EventTime时才有效; 在时间语义为ProcessingTime时,watermark将会被忽略;在时间语义为
         * IngestionTime时,watermark会被自动产生的ingestion time watermark替换
         * <p>This method is only relevant when running on {@link TimeCharacteristic#EventTime}. On
         * {@link TimeCharacteristic#ProcessingTime},Watermarks will be ignored. On {@link
         * TimeCharacteristic#IngestionTime}, the Watermarks will be replaced by the automatic
         * ingestion time watermarks.
         *
         * @param mark The Watermark to emit
         */
        @PublicEvolving
        void emitWatermark(Watermark mark);

        /**
         * 将Source标记为临时空闲.这会告诉系统Source将在无限期内暂时停止发射element和watermark.
         * 这仅在IngestionTime和EventTime时间语义上运行时才有效，以允许下游任务推进其watermark，
         * 而无需在下游任务空闲时等待来自Source的watermark。
         * Marks the source to be temporarily idle. This tells the system that this source will
         * temporarily stop emitting records and watermarks for an indefinite amount of time. This
         * is only relevant when running on {@link TimeCharacteristic#IngestionTime} and {@link
         * TimeCharacteristic#EventTime}, allowing downstream tasks to advance their watermarks
         * without the need to wait for watermarks from this source while it is idle.
         *
         * SourceFunction一开始就应尽最大努力调用此方法，一向系统说明自己处于空闲状态。
         * 一旦调用SourceContext#Collect(T)、SourceContext#collectWithTimestamp(T，Long)}或
         * SourceContext#emitWatermark(Watermark)}从Source发射element或watermark，系统将考虑再次
         * 恢复Source处于活动状态。
         * <p>Source functions should make a best effort to call this method as soon as they
         * acknowledge themselves to be idle. The system will consider the source to resume activity
         * again once {@link SourceContext#collect(T)}, {@link SourceContext#collectWithTimestamp(T,
         * long)}, or {@link SourceContext#emitWatermark(Watermark)} is called to emit elements or
         * watermarks from the source.
         */
        @PublicEvolving
        void markAsTemporarilyIdle();

        /**
         * 返回checkpoint lock,有关更多的关于如何编写一致性检查点的Source,详见SourceFunction类级别的注释
         * Returns the checkpoint lock. Please refer to the class-level comment in {@link
         * SourceFunction} for details about how to write a consistent checkpointed source.
         *
         * @return The object to use as the lock
         */
        Object getCheckpointLock();

        // 系统调用该方法来关闭上下文context
        /** This method is called by the system to shut down the context. */
        void close();
    }
}
