bin_dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
at_home=$( cd "$( dirname "$bin_dir" )" && pwd )

function build_shade_jar() {
  export at_shaded_jar="/$at_home/core/target/accumulo-testing-core-$at_version-shaded.jar"
  export CLASSPATH="$at_shaded_jar:$CLASSPATH"
  if [ ! -f "$at_shaded_jar" ]; then
    echo "Building $at_shaded_jar"
    cd "$at_home" || exit 1
    mvn clean package -P create-shade-jar -D skipTests -D accumulo.version=$(get_version "ACCUMULO") -D hadoop.version=$(get_version "HADOOP") -D zookeeper.version=$(get_version "ZOOKEEPER")
  fi
}

# TODO need to check if cluster-control.sh exists
. $at_home/conf/cluster-control.sh
build_shade_jar
CLASSPATH="$at_home/conf:$at_home/core/target/accumulo-testing-core-$at_version-shaded.jar"
perf_pkg="org.apache.accumulo.testing.core.performance.impl"
case "$1" in 
  run)
   start_cluster
    java -Dlog4j.configuration="file:$log4j_config" ${perf_pkg}.ListTests | while read test_class; do
      pt_tmp=$(mktemp -d -t accumulo_pt_XXXXXXX)
      setup_accumulo
      get_config_file accumulo-site.xml "$pt_tmp"
      java -Dlog4j.configuration="file:$log4j_config"  ${perf_pkg}.MergeSiteConfig "$test_class" "$pt_tmp"
      put_config_file "$pt_tmp/accumulo-site.xml"
      put_server_code "$at_home/core/target/accumulo-testing-core-$at_version.jar" 
      start_accumulo
      get_config_file accumulo-client.properties "$pt_tmp"
      java -Dlog4j.configuration="file:$log4j_config"  ${perf_pkg}.PerfTestRunner "$pt_tmp/accumulo-client.properties" "$test_class" "$(get_version 'ACCUMULO')" "$2"
    done
    stop_cluster
    ;;
  compare)
    java -Dlog4j.configuration="file:$log4j_config"  ${perf_pkg}.Compare "$2" "$3"
    ;;
  csv)
    java -Dlog4j.configuration="file:$log4j_config"  ${perf_pkg}.Csv "${@:2}"
    ;;
  *)
    echo "Unknown command : $1"
    print_usage
    exit 1
esac
 
