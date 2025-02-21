use clap::Parser;
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::{self, Write, Read};
use std::collections::HashMap;


// Create a new database stucture for storing all the json data


/// Manages inputs, outputs and the command to run
#[derive(Serialize, Deserialize, Debug)]
struct CalculationManager {}


/// Manages copy history 
#[derive(Serialize, Deserialize, Debug)]
struct CopyManager {}

/// Describes a calculation node in a graph
#[derive(Serialize, Deserialize, Debug)]
struct CalculationNode {
    git_hash: String,
    calculation: CalculationManager,
    copy: CopyManager,
}

#[derive(Serialize, Deserialize, Debug)]
struct DataNode {
    save: bool,
    copy: CopyManager,
}

#[derive(Serialize, Deserialize, Debug)]
struct CopyNode {

}


#[derive(Serialize, Deserialize, Debug)]
struct JsonStorage {
    calculation_nodes: HashMap<String, CalculationNode>,
    data_nodes: HashMap<String, DataNode>,
    copy_nodes: HashMap<String, CopyNode>,
}




// Database schema
#[derive(Serialize, Deserialize, Debug)]
struct Node {
    name: String,
    count: i32,
}

#[derive(Serialize, Deserialize, Debug)]
struct Database {
    node_list: Vec<Node>
}

// implement reading and writing to the database.
impl Database {

    fn write_database(&mut self, filename: &str) -> Result<(), io::Error>{
        let mut file = File::create(filename)?; 
        
        let write_string = match serde_json::to_string_pretty(self){
            Ok(string) => string,
            Err(e) => panic!("Failed to serialize the databes. Aborting!")
        };
        file.write_all(write_string.as_bytes())?;
        Ok(())
    }
    
}

fn read_json_file(filename: &str) -> std::io::Result<Database> {
    let mut file = File::open(filename)?; // Open the file
    let mut contents = String::new();
    file.read_to_string(&mut contents)?; // Read file into a string
    let db: Database = serde_json::from_str(&contents)?; // Deserialize JSON
    Ok(db)
}





/// Command line interface
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(long)]
    name: String,

    /// Number of times to greet
    #[arg(short, long, default_value_t = 1)]
    count: i32,

    #[arg(long)]
    file: String
}


fn main() {
    // Read the arguments
    let args: Args = Args::parse();


    // Read the data from a file;
    let mut db = read_json_file((&args.file)).expect("Failed to read the database. Aborting!");

    // Creat the new node object
    let node = Node{count: args.count, name: args.name};

    // Extend the database
    db.node_list.push(node);

    // Write the database to the file

    db.write_database(&args.file).expect("Failed writing to the database.");
}