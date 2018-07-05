# internal functions just for uno cluster control 

function get_ah {
  echo "$(uno env | grep ACCUMULO_HOME | sed 's/export ACCUMULO_HOME=//' | sed 's/"//g')"
}


# functions required for accumulo testing cluster control

function start_cluster {
  uno setup accumulo
}

function setup_accumulo {
  uno setup accumulo --no-deps 
}

function get_config_file {
  local ah=$(get_ah)
  cp "$ah/conf/$1" "$2"
}

function put_config_file {
  local ah=$(get_ah)
  cp "$1" "$ah/conf"
}

function put_server_code {
  local ah=$(get_ah)
  cp "$1" "$ah/lib/ext"
}

function start_accumulo {
  uno stop accumulo --no-deps
  uno start accumulo --no-deps
}

function stop_cluster {
  uno kill
}


