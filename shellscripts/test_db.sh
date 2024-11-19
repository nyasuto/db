#!/usr/bin/env bats

setup_file() {
    # Create a temporary database file
    export TEST_DB="test_db" #$(mktemp)
    # Redirect the database file to the temporary file
    export DATABASE_FILE=$TEST_DB
}

teardown() {
    # Remove the temporary database file
    echo ""    
    #rm -f "$TEST_DB"
}

@test "db_set adds a record to the database" {
    run bash -c 'source db.sh; DATABASE_FILE="$TEST_DB" db_set 42 "{\"name\": \"San Francisco\", \"attractions\": [\"Exploratorium\", \"Golden Gate Bridge\"]}"'
    [ "$status" -eq 0 ]
}

@test "db_get retrieves a record from the database" {
    # echo '42,{"name": "San Francisco", "attractions": ["Exploratorium", "Golden Gate Bridge"]}' > "$DB_FILE"
    run bash -c 'source db.sh; DATABASE_FILE="$TEST_DB" db_get 42'
    [ "$status" -eq 0 ]
    [ "$output" = '{"name": "San Francisco", "attractions": ["Exploratorium", "Golden Gate Bridge"]}' ]
}


@test "db_set and db_get with large volume of data" {
    # Insert a large number of records
    for i in $(seq 1 10000); do
        run bash -c "source db.sh; DATABASE_FILE=\"$TEST_DB\" db_set key$i value$i"
        [ "$status" -eq 0 ]
    done

    # Retrieve a few records to check performance
    for i in $(seq 1 10000 1000); do
        run bash -c "source db.sh; DATABASE_FILE=\"$TEST_DB\" db_get key$i"
        [ "$status" -eq 0 ]
        [ "$output" = "value$i" ]
    done
}