use clap::{Parser,Subcommand};
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::{self, Write, Read};
use std::collections::HashMap;
use regex::Regex;

// Create a new database stucture for storing all the json data


/// Manages inputs, outputs and the command to run
#[derive(Serialize, Deserialize, Default, Debug)]
struct CalculationManager {
    inputs: Vec<String>,
    outputs: Vec<String>,
    program: String
}


/// Manages copy history 
#[derive(Serialize, Deserialize, Default, Debug)]
struct CopyManager {
    name: String, // Name of the copy node
    origin: String // Name of the origin node
}

/// Describes a calculation node in a graph
#[derive(Serialize, Deserialize, Default, Debug)]
struct CalculationNode {
    git_hash: String,
    tags: Vec<String>, // For stornig things like the experiment or other thigs. 
    calculation: CalculationManager,
    copy: CopyManager,
}

#[derive(Serialize, Deserialize, Default, Debug)]
struct DataNode {
    save: bool,
    tags: Vec<String>,
    copy: CopyManager,
}

/// The main class that defines the whole data storage structure.
#[derive(Serialize, Deserialize, Default, Debug)]
struct JsonStorage {
    calculation_nodes: HashMap<String, CalculationNode>,
    data_nodes: HashMap<String, DataNode>,
}



// implement reading and writing to the database.

impl JsonStorage {

    fn write_database(&self, filename: &str) -> Result<(), io::Error>{
        let mut file = File::create(filename)?; 
        
        let write_string = match serde_json::to_string_pretty(self){
            Ok(string) => string,
            Err(e) => panic!("Failed to serialize the databes. Aborting!")
        };
        file.write_all(write_string.as_bytes())?;
        Ok(())
    }
    
    /// Add a new calculation to the database
    fn add_calculation(&self, base_name: &String, command_string: & String ) {
        
        // Extract inputs and outputs
        let input_re = Regex::new(r"input\((.*?)\)").expect("failed at creating a regular expression.");
        let output_re = Regex::new(r"output\((.*?)\)").expect("Failed at creating regulary expression."); // Match 'output(file)'
        
        let inputs: Vec<String> = input_re
            .captures_iter(command_string)
            .map(|cap| cap[1].to_string()) // Get the file name without 'input()'
            .collect();

        let outputs: Vec<String> = output_re
            .captures_iter(command_string)
            .map(|cap| cap[1].to_string()) // Get the file name without 'input()'
            .collect();

        // Format the command string

        let mut final_command = command_string.clone();

        for (i, value) in inputs.iter().enumerate() {
            final_command = final_command.replace(&format!("input({})",value), &format!("input_{}", i));
        }
        
        for (i, value) in outputs.iter().enumerate() {
            final_command = final_command.replace(&format!("output({})",value), &format!("output_{}", i));
        }

    }


}

fn read_json_file(filename: &str) -> std::io::Result<JsonStorage> {
    let mut file = File::open(filename)?; // Open the file
    let mut contents = String::new();
    file.read_to_string(&mut contents)?; // Read file into a string
    let db: JsonStorage = serde_json::from_str(&contents)?; // Deserialize JSON
    Ok(db)
}





/// Command line interface
#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Name of the person to greet
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Adds files to myapp
    Init,
    ReadData,
    AddCalculation {name: String, command : String},

}



fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Init  => {
            let calculation_manager = CalculationManager{inputs:vec!["input_1".to_string()],
                    outputs: vec!["output_2".to_string()],
                    program: "python3 input_1 output_2".to_string()};

            let copy_manager = CopyManager {name: "test".to_string(), origin: "another_test".to_string()};
            
            let mut calculation_nodes = HashMap::new();
            calculation_nodes.insert("test".to_string(), CalculationNode{git_hash: "".to_string(), tags: Vec::new(), calculation: calculation_manager, copy: copy_manager});
            let mut data_nodes = HashMap::new();
            data_nodes.insert("test_data".to_string(), DataNode{save:true, tags:Vec::new(), copy: CopyManager::default()});                                                
            let mut default_struct = JsonStorage{calculation_nodes: calculation_nodes, data_nodes: data_nodes};
            default_struct.write_database(&"test.json".to_string());
        }
        Commands::ReadData => {
            let db = read_json_file("test.json").expect("Failed to read the database");
            println!("{}", serde_json::to_string(&db).expect("failed to seriazile the code"));
        }
        Commands::AddCalculation {name, command} => {
            let db = read_json_file("test.json").expect("Failed to read the database");
            db.add_calculation(name, command);
            db.write_database(&"test.json".to_string()).expect("failed to write the database.")
        }
    
    }
}