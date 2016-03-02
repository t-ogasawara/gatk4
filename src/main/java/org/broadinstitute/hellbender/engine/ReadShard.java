package org.broadinstitute.hellbender.engine;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.util.Locatable;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.iterators.ReadFilteringIterator;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A class to represent a shard of reads data, optionally expanded by a configurable amount of padded data.
 *
 * The reads are lazily loaded by default (when accessing the reads via {@link #iterator}. Loading all the
 * reads in the window at once is possible via {@link #loadAllReads}.
 *
 * The reads returned will overlap the expanded padded interval. It's possible to query whether they are within
 * the main part of the shard via {@link #isContainedWithinShard} and {@link #startsWithinShard}.
 *
 * The reads in the window can be filtered via {@link #setReadFilter} (no filtering is performed by default).
 */
public final class ReadShard implements Iterable<GATKRead>, Locatable {

    private final SimpleInterval interval;
    private final SimpleInterval paddedInterval;
    private final ReadsDataSource readsSource;
    private ReadFilter readFilter;

    /**
     * Create a new ReadShard spanning the specified interval, with the specified amount of padding.
     *
     * @param interval the genomic span covered by this shard
     * @param paddedInterval the span covered by this shard, plus any additional padding on each side (must contain the un-padded interval)
     * @param readsSource source of reads from which to populate this shard
     */
    public ReadShard( final SimpleInterval interval, final SimpleInterval paddedInterval, final ReadsDataSource readsSource ) {
        Utils.nonNull(interval);
        Utils.nonNull(paddedInterval);
        Utils.nonNull(readsSource);

        if ( ! paddedInterval.contains(interval) ) {
            throw new IllegalArgumentException("The padded interval must contain the un-padded interval");
        }

        this.interval = interval;
        this.paddedInterval = paddedInterval;
        this.readsSource = readsSource;
    }

    /**
     * Reads in this shard will be filtered using this filter before being returned
     *
     * @param filter filter to use (may be null)
     */
    public void setReadFilter( final ReadFilter filter ) {
        this.readFilter = filter;
    }

    /**
     * @return Contig this shard belongs to
     */
    @Override
    public String getContig() {
        return interval.getContig();
    }

    /**
     * @return Start position of this shard
     */
    @Override
    public int getStart() {
        return interval.getStart();
    }

    /**
     * @return End position of this shard
     */
    @Override
    public int getEnd() {
        return interval.getEnd();
    }

    /**
     * @return the interval this shard spans
     */
    public SimpleInterval getInterval() {
        return interval;
    }

    /**
     * @return the interval this shard spans, potentially with additional padding on each side
     */
    public SimpleInterval getPaddedInterval() {
        return paddedInterval;
    }

    /**
     * @return an iterator over reads in this shard, as filtered using the configured read filter;
     *         reads are lazily loaded rather than pre-loaded
     */
    @Override
    public Iterator<GATKRead> iterator() {
        final Iterator<GATKRead> readsIterator = readsSource.query(paddedInterval);

        return readFilter != null ? new ReadFilteringIterator(readsIterator, readFilter) : readsIterator;
    }

    /**
     * @return a List containing all reads in this shard, pre-loaded, and filtered using the configured read filter
     *
     * note: call {@link #iterator} instead to avoid pre-loading all reads at once
     */
    public List<GATKRead> loadAllReads() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED), false).collect(Collectors.toList());
    }

    /**
     * @param loc Locatable to test
     * @return true if loc is contained within this shard's interval, otherwise false
     */
    public boolean isContainedWithinShard( final Locatable loc ) {
        return interval.contains(loc);
    }

    /**
     * @param loc Locatable to test
     * @return true if loc starts within this shard's interval, otherwise false
     */
    public boolean startsWithinShard( final Locatable loc ) {
        return interval.contains(new SimpleInterval(loc.getContig(), loc.getStart(), loc.getStart()));
    }

    /**
     * @return number of bases of padding to the left of our interval
     */
    public int numLeftPaddingBases() {
        return interval.getStart() - paddedInterval.getStart();
    }

    /**
     * @return number of bases of padding to the right of our interval
     */
    public int numRightPaddingBases() {
        return paddedInterval.getEnd() - interval.getEnd();
    }

    /**
     * Shard an interval into ReadShards using the shardSize and shardPadding arguments
     *
     * @param interval interval to shard; must be on the contig according to the provided dictionary
     * @param shardSize desired shard size; intervals larger than this will be divided into shards of up to this size
     * @param shardPadding desired shard padding; each shard's interval will be padded on both sides by this number of bases
     * @param readsSource data source for reads
     * @param dictionary sequence dictionary for reads
     * @return List of {@link ReadShard} objects, sharded and padded as necessary
     */
    public static List<ReadShard> shardInterval( final SimpleInterval interval, final int shardSize, final int shardPadding, final ReadsDataSource readsSource, final SAMSequenceDictionary dictionary ) {
        if ( ! IntervalUtils.intervalIsOnDictionaryContig(interval, dictionary) ) {
            throw new IllegalArgumentException("Interval " + interval + " not within the bounds of a contig in the provided dictionary");
        }

        final List<ReadShard> shards = new ArrayList<>();
        int start = interval.getStart();

        while ( start <= interval.getEnd() ) {
            int end = Math.min(start + shardSize - 1, interval.getEnd());

            final SimpleInterval nextShardInterval = new SimpleInterval(interval.getContig(), start, end);
            final SimpleInterval nextShardIntervalPadded = nextShardInterval.expandWithinContig(shardPadding, dictionary);
            shards.add(new ReadShard(nextShardInterval, nextShardIntervalPadded, readsSource));

            start += shardSize;
        }

        return shards;
    }
}
