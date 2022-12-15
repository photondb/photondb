#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
# REQUIRE: db_bench binary exists in the current directory

# Exit Codes
EXIT_INVALID_ARGS=1
EXIT_NOT_COMPACTION_TEST=2
EXIT_UNKNOWN_JOB=3

# Size Constants
K=1024
M=$((1024 * K))
G=$((1024 * M))
T=$((1024 * G))

function display_usage() {
  echo "usage: benchmark.sh [--help] <test>"
  echo ""
  echo "These are the available benchmark tests:"
  echo -e "\tfillseq_disable_wal"
  echo -e "\tbulkload"
  echo -e "\tupdaterandom"
  echo -e "\treadrandom"
  echo -e "\twaitforreclaiming"
  echo -e "\tdebug"
  echo ""
  echo "Generic enviroment Variables:"
  echo -e "\tJOB_ID\t\t\t\tAn identifier for the benchmark job, will appear in the results"
  echo -e "\tDB_DIR\t\t\t\tPath to write the database data directory"
  echo -e "\tOUTPUT_DIR\t\t\tPath to write the benchmark results to (default: /tmp)"
  echo -e "\tNUM_KEYS\t\t\tThe number of keys to use in the benchmark"
  echo -e "\tKEY_SIZE\t\t\tThe size of the keys to use in the benchmark (default: 20 bytes)"
  echo -e "\tVALUE_SIZE\t\t\tThe size of the values to use in the benchmark (default: 400 bytes)"
  echo -e "\tPAGE_SIZE\t\t\tThe size of the database page in the benchmark (default: 8 KB)"
  echo -e "\tNUMACTL\t\t\t\tWhen defined use numactl --interleave=all"
  echo -e "\tNUM_THREADS\t\t\tThe number of threads to use (default: 64)"
  echo -e "\tCACHE_SIZE\t\t\tSize of the block cache (default: 16GB)"
  echo -e "\tBOTTOMMOST_COMPRESSION\t\t(default: none)"
  echo -e "\tDURATION\t\t\tNumber of seconds for which the test runs"
  echo -e "\tWRITES\t\t\t\tNumber of writes for which the test runs"
  echo -e "\tWRITE_BUFFER_SIZE_MB\t\tThe size of the write buffer in MB (default: 128)"
  echo -e "\tDISABLE_SPACE_RECLAIMING\tDisable space reclamation (default: false)"
  echo -e "\tMAX_SPACE_AMP\t\t\tThe max percent of space amplification to reclaim space (default: 10)"
  echo -e "\tSPACE_USED_HIGH\t\t\tThe space watermark which the DB needed to reclaim, in bytes (default: 100G)"
  echo -e "\tUSE_O_DIRECT\t\t\tUse O_DIRECT for user reads and compaction"
  echo -e "\tSTATS_INTERVAL_SECONDS\t\tValue for stats_interval_seconds"
  echo -e "\tREPORT_INTERVAL_SECONDS\t\tValue for report_interval_seconds"
}

if [ $# -lt 1 ]; then
  display_usage
  exit $EXIT_INVALID_ARGS
fi
bench_cmd=$1
shift
bench_args=$*

if [[ "$bench_cmd" == "--help" ]]; then
  display_usage
  exit
fi

job_id=${JOB_ID}

if [ -z $DB_DIR ]; then
  echo "DB_DIR is not defined"
  exit $EXIT_INVALID_ARGS
fi

output_dir=${OUTPUT_DIR:-/tmp}
if [ ! -d $output_dir ]; then
  mkdir -p $output_dir
fi

report="$output_dir/report.tsv"
schedule="$output_dir/schedule.txt"

num_threads=${NUM_THREADS:-64}
# Only for tests that do range scans
cache_size=${CACHE_SIZE:-$(( 16 * $G ))}

duration=${DURATION:-0}
writes=${WRITES:-0}

num_keys=${NUM_KEYS:-8000000000}
key_size=${KEY_SIZE:-20}
value_size=${VALUE_SIZE:-400}
page_size=${PAGE_SIZE:-8192}
write_buffer_mb=${WRITE_BUFFER_SIZE_MB:-128}
space_used_high=${SPACE_USED_HIGH:-107374182400}
stats_interval_seconds=${STATS_INTERVAL_SECONDS:-60}
report_interval_seconds=${REPORT_INTERVAL_SECONDS:-1}
max_space_amp=${MAX_SPACE_AMP:-10}
disable_space_reclaiming=${DISABLE_SPACE_RECLAIMING:-0}

# o_direct_flags=""
# if [ ! -z $USE_O_DIRECT ]; then
#   # Some of these flags are only supported in new versions and --undefok makes that work
#   o_direct_flags="--use_direct_reads --use_direct_io_for_flush_and_compaction --prepopulate_block_cache=1"
# fi

const_params_base="
  --db=$DB_DIR \
  \
  --num=$num_keys \
  --key-size=$key_size \
  --value-size=$value_size \
  --page-size=$page_size \
  --cache-size=$cache_size \
  \
  --write-buffer-size=$(( $write_buffer_mb * M)) \
  --space-used-high=$space_used_high \
  --max-space-amplification-percent=$max_space_amp \
  \
  --verify-checksum=1 \
  \
  --stats-interval-sec=$stats_interval_seconds \
  --hist \
  --db-stats \
  \
  $bench_args"

const_params="$const_params_base "

if [ $disable_space_reclaiming = 1 ] || [ $disable_space_reclaiming = true ]; then
    const_params="$const_params --disable-space-reclaiming"
fi

# You probably don't want to set both --writes and --duration
if [ $duration -gt 0 ]; then
  const_params="$const_params --duration=$duration"
fi
if [ $writes -gt 0 ]; then
  const_params="$const_params --writes=$writes"
fi

params_w="$const_params "

params_bulkload="$const_params "

params_fillseq="$params_w "
tsv_header="ops_sec\tmb_sec\tw_amp\tusec_op\tp50\tp99\tp99.9\tp99.99\tmin\tmax\tavg\tTstall\tNstall\tu_cpu\ts_cpu\trss\ttest\tversion\tjob_id\tgithash"

function get_cmd() {
  output=$1

  numa=""
  if [ ! -z $NUMACTL ]; then
    numa="numactl --interleave=all "
  fi

  # Try to use timeout when duration is set because some tests (revrange*) hang
  # for some versions (v6.10, v6.11).
  timeout_cmd=""
  if [ $duration -gt 0 ]; then
    if hash timeout ; then
      timeout_cmd="timeout $(( $duration + 600 ))"
    fi
  fi

  echo "/usr/bin/time -f '%e %U %S' -o $output $numa $timeout_cmd"
}

function month_to_num() {
    local date_str=$1
    date_str="${date_str/Jan/01}"
    date_str="${date_str/Feb/02}"
    date_str="${date_str/Mar/03}"
    date_str="${date_str/Apr/04}"
    date_str="${date_str/May/05}"
    date_str="${date_str/Jun/06}"
    date_str="${date_str/Jul/07}"
    date_str="${date_str/Aug/08}"
    date_str="${date_str/Sep/09}"
    date_str="${date_str/Oct/10}"
    date_str="${date_str/Nov/11}"
    date_str="${date_str/Dec/12}"
    echo $date_str
}

function start_stats {
  output=$1
  iostat -y -mx 1  >& $output.io &
  vmstat 1 >& $output.vm &
  # tail -1 because "ps | grep db_bench" returns 2 entries and we want the second
  while :; do ps aux | grep bench | grep -v grep | tail -1; sleep 10; done >& $output.ps &
  # This sets a global value
  pspid=$!

  while :; do
    m_gb=$( ls -l $DB_DIR 2> /dev/null | grep map | awk '{ c += 1; b += $5 } END { printf "%.1f", b / (1024*1024*1024) }' )
    d_gb=$( ls -l $DB_DIR 2> /dev/null | grep dat | awk '{ c += 1; b += $5 } END { printf "%.1f", b / (1024*1024*1024) }' )
    a_gb=$( ls -l $DB_DIR 2> /dev/null | awk '{ c += 1; b += $5 } END { printf "%.1f", b / (1024*1024*1024) }' )
    ts=$( date +%H%M%S )
    echo -e "${a_gb}\t${d_gb}\t${m_gb}\t${ts}"
    sleep 10
  done >& $output.sizes &
  # This sets a global value
  szpid=$!
}

function stop_stats {
  output=$1
  kill $pspid
  kill $szpid
  killall iostat
  killall vmstat
  sleep 1
  gzip $output.io
  gzip $output.vm

  am=$( sort -nk 1,1 $output.sizes | tail -1 | awk '{ print $1 }' )
  dm=$( sort -nk 2,2 $output.sizes | tail -1 | awk '{ print $2 }' )
  mm=$( sort -nk 3,3 $output.sizes | tail -1 | awk '{ print $3 }' )
  echo -e "max sizes (GB): $am all, $dm pages, $mm mapping" >> $output.sizes
}

function units_as_gb {
  size=$1
  units=$2

  case $units in
    MB)
      echo "$size" | awk '{ printf "%.1f", $1 / 1024.0 }'
      ;;
    GB)
      echo "$size"
      ;;
    TB)
      echo "$size" | awk '{ printf "%.1f", $1 * 1024.0 }'
      ;;
    *)
      echo "NA"
      ;;
  esac
}

function summarize_result {
  test_out=$1
  test_name=$2
  bench_name=$3

  # In recent versions these can be found directly via db_bench --version, --build_info but
  # grepping from the log lets this work on older versions.
  version="0.0.0"
  git_hash="n/a"


  nstall_writes=$( grep "^BufferSet" $test_out| tail -1  | awk '{  print $3 }' )
  stall_interval_ms=$( grep "^BufferSet" $test_out| tail -1  | awk '{  print $5 }' )

  if ! grep ^"$bench_name" "$test_out" > /dev/null 2>&1 ; then
    echo -e "failed $bench_name $test_out\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t$test_name\t$version\t$job_id\t$git_hash"
    return
  fi

  # Output formats
  # fillseq    :       7.854 micros/op 127320 ops/sec 1800.001 seconds 229176999 operations;   51.0 MB/s
  ops_sec=$( grep ^"$bench_name" "$test_out" | awk '{ print $5 }' )
  usecs_op=$( grep ^"$bench_name" "$test_out" | awk '{ printf "%.1f", $3 }' )
  mb_sec=$( grep ^"$bench_name" "$test_out" | awk '{ print $11 }' )

  wamp=$( grep "^TableStats" $test_out | tail -1 | awk '{ printf "%.1f", $7 }' )

  p50=$( grep "^Percentiles" $test_out | tail -1 | awk '{ printf "%.1f", $4 }' )
  p99=$( grep "^Percentiles" $test_out | tail -1 | awk '{ printf "%.0f", $10 }' )
  p999=$( grep "^Percentiles" $test_out | tail -1 | awk '{ printf "%.0f", $13 }' )
  p9999=$( grep "^Percentiles" $test_out | tail -1 | awk '{ printf "%.0f", $16 }' )
  min=$( grep "^Percentiles" $test_out | tail -1 | awk '{ printf "%.0f", $19 }' )
  max=$( grep "^Percentiles" $test_out | tail -1 | awk '{ printf "%.0f", $22 }' )
  avg=$( grep "^Percentiles" $test_out | tail -1 | awk '{ printf "%.0f", $25 }' )

  # Use the last line because there might be extra lines when the db_bench process exits with an error
  time_out="$test_out".time
  u_cpu=$( tail -1 "$time_out" | awk '{ printf "%.1f", $2 / 1000.0 }' )
  s_cpu=$( tail -1 "$time_out" | awk '{ printf "%.1f", $3 / 1000.0  }' )

  rss="NA"
  if [ -f $test_out.stats.ps ]; then
    rss=$( awk '{ printf "%.1f\n", $6 / (1024 * 1024) }' "$test_out".stats.ps | sort -n | tail -1 )
  fi

  # if the report TSV (Tab Separate Values) file does not yet exist, create it and write the header row to it
  if [ ! -f "$report" ]; then
    echo -e "# ops_sec - operations per second" >> "$report"
    echo -e "# mb_sec - ops_sec * size-of-operation-in-MB" >> "$report"
    echo -e "# w_amp - Write-amplification as (bytes written by compaction / bytes written by memtable flush)" >> "$report"
    echo -e "# usec_op - Microseconds per operation" >> "$report"
    echo -e "# p50, p99, p99.9, p99.99, max, min, avg - 50th, 99th, 99.9th, 99.99th percentile and max min avg response time in usecs" >> "$report"
    echo -e "# Tstall - stall interval" >> "$report"
    echo -e "# Nstall - Number of write stalls" >> "$report"
    echo -e "# u_cpu - #seconds/1000 of user CPU" >> "$report"
    echo -e "# s_cpu - #seconds/1000 of system CPU" >> "$report"
    echo -e "# rss - max RSS in GB for db_bench process" >> "$report"
    echo -e "# test - Name of test" >> "$report"
    echo -e "# version - Photondb version" >> "$report"
    echo -e "# job_id - User-provided job ID" >> "$report"
    echo -e "# githash - git hash at which db_bench was compiled" >> "$report"
    echo -e $tsv_header >> "$report"
  fi

  echo -e "$ops_sec\t$mb_sec\t$wamp\t$usecs_op\t$p50\t$p99\t$p999\t$p9999\t$min\t$max\t$avg\t$stall_interval_ms\t$nstall_writes\t$u_cpu\t$s_cpu\t$rss\t$test_name\t$version\t$job_id\t$git_hash" \
    >> "$report"
}

function run_fillseq {
  # This runs with a vector memtable. WAL can be either disabled or enabled
  # depending on the input parameter (1 for disabled, 0 for enabled). The main
  # benefit behind disabling WAL is to make loading faster. It is still crash
  # safe and the client can discover where to restart a load after a crash. I
  # think this is a good way to load.

  # Make sure that we'll have unique names for all the files so that data won't
  # be overwritten.
  if [ $1 == 1 ]; then
    log_file_name="${output_dir}/benchmark_fillseq.wal_disabled.v${value_size}.log"
    test_name=fillseq.wal_disabled.v${value_size}
  else
    log_file_name="${output_dir}/benchmark_fillseq.wal_enabled.v${value_size}.log"
    test_name=fillseq.wal_enabled.v${value_size}
  fi

  echo "Loading $num_keys keys sequentially"
  time_cmd=$( get_cmd $log_file_name.time )
  cmd="$time_cmd ./target/release/photondb-tools bench --benchmarks=fillseq \
       $params_fillseq \
       --use-existing-db=0 \
       --threads=1 \
       --seed-base=$( date +%s ) \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  start_stats $log_file_name.stats
  eval $cmd
  stop_stats $log_file_name.stats

  # The constant "fillseq" which we pass to db_bench is the benchmark name.
  summarize_result $log_file_name $test_name fillseq
}

function run_bulkload {
  echo "Bulk loading $num_keys random keys"
  log_file_name=$output_dir/benchmark_bulkload_fillrandom.t${num_threads}.log
  time_cmd=$( get_cmd $log_file_name.time )
  cmd="$time_cmd ./target/release/photondb-tools bench --benchmarks=fillrandom \
       --use-existing-db=0 \
       $params_bulkload \
       --threads=${num_threads} \
       --seed-base=$( date +%s ) \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  start_stats $log_file_name.stats
  eval $cmd
  stop_stats $log_file_name.stats
  summarize_result $log_file_name bulkload fillrandom
}

function run_change {
  output_name=$1
  grep_name=$2
  benchmarks=$3
  echo "Do $num_keys random $output_name"
  log_file_name="$output_dir/benchmark_${output_name}.t${num_threads}.s${syncval}.log"
  time_cmd=$( get_cmd $log_file_name.time )
  cmd="$time_cmd ./target/release/photondb-tools bench --benchmarks=$benchmarks \
        $params_w \
       --use-existing-db=1 \
       --threads=$num_threads \
       --seed-base=$( date +%s ) \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  start_stats $log_file_name.stats
  eval $cmd
  stop_stats $log_file_name.stats
  summarize_result $log_file_name ${output_name}.t${num_threads}.s${syncval} $grep_name
}

function run_readrandom {
  echo "Reading $num_keys random keys"
  log_file_name="${output_dir}/benchmark_readrandom.t${num_threads}.log"
  time_cmd=$( get_cmd $log_file_name.time )
  cmd="$time_cmd ./target/release/photondb-tools bench --benchmarks=readrandom \
        $params_w \
       --use-existing-db=1 \
       --threads=$num_threads \
       --seed-base=$( date +%s ) \
       2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  start_stats $log_file_name.stats
  eval $cmd
  stop_stats $log_file_name.stats
  summarize_result $log_file_name readrandom.t${num_threads} readrandom
}

function run_waitforreclaiming {
  echo "Wait for reclaiming"
  log_file_name="${output_dir}/benchmark_waitforreclaiming.log"
  time_cmd=$( get_cmd $log_file_name.time )
  cmd="$time_cmd ./target/release/photondb-tools bench --benchmarks=waitforreclaiming \
        $params_w \
        --use-existing-db=1 \
        --seed-base=$( date +%s ) \
        2>&1 | tee -a $log_file_name"
  if [[ "$job_id" != "" ]]; then
    echo "Job ID: ${job_id}" > $log_file_name
    echo $cmd | tee -a $log_file_name
  else
    echo $cmd | tee $log_file_name
  fi
  start_stats $log_file_name.stats
  eval $cmd
  stop_stats $log_file_name.stats
  summarize_result $log_file_name waitforreclaiming waitforreclaiming
}

function now() {
  echo `date +"%s"`
}


echo "===== Benchmark ====="

# Run!!!
IFS=',' read -a jobs <<< $bench_cmd
# shellcheck disable=SC2068
for job in ${jobs[@]}; do

  if [ $job != debug ]; then
    echo "Starting $job (ID: $job_id) at `date`" | tee -a $schedule
  fi

  start=$(now)
  if [ $job = fillseq_disable_wal ]; then
    run_fillseq 1
  elif [ $job = bulkload ]; then
    run_bulkload
  elif [ $job = updaterandom ]; then
    run_change updaterandom updaterandom updaterandom
  elif [ $job = readrandom ]; then
    run_readrandom
  elif [ $job = waitforreclaiming ]; then
    run_waitforreclaiming
  elif [ $job = debug ]; then
    num_keys=1000; # debug
    echo "Setting num_keys to $num_keys"
  else
    echo "unknown job $job"
    exit $EXIT_UNKNOWN_JOB
  fi
  end=$(now)

  if [ $job != debug ]; then
    echo "Completed $job (ID: $job_id) in $((end-start)) seconds" | tee -a $schedule
  fi

  echo -e $tsv_header
  tail -1 $report

done