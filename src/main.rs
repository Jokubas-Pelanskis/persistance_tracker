use clap::{Parser,Subcommand};
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::{self, Write, Read};
use std::collections::HashMap;
use regex::Regex;
use std::fmt;
// Create a new database stucture for storing all the json data


/// Manages inputs, outputs and the command to run
#[derive(Serialize, Deserialize, Default, Debug)]
struct CalculationManager {
    inputs: Vec<String>,
    outputs: Vec<String>,
    program: String
}


impl CalculationManager {
    /// Generate the command to run the calculation.
    fn get_full_program(& self) -> String {
        let mut final_command = self.program.clone();

        for (i, value) in self.outputs.iter().enumerate() {
            final_command = final_command.replace(&format!("output_{}", i), value);
        }
        for (i, value) in self.inputs.iter().enumerate() {
            final_command = final_command.replace(&format!("input_{}", i),value);
        }
        final_command

    }
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

impl fmt::Display for CalculationNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), std::fmt::Error>{

        let full_program = self.calculation.get_full_program();

        write!(f, "generic program: \n{} \nfull program:\n{}\n", self.calculation.program, full_program)
    }
}


#[derive(Serialize, Deserialize, Default, Debug)]
struct DataNode {
    save: bool,
    tags: Vec<String>,
    copy: CopyManager,
}


/// add a trait for adding tags
trait NodeTags {
    fn add_tags(&mut self, tag_list:Vec<String>){}
}

impl NodeTags for DataNode {
    fn add_tags(&mut self, tag_list:Vec<String>){
        for item in tag_list {
            if !self.tags.contains(&item) {
                self.tags.push(item.clone());
            }
        }
    }
}

impl NodeTags for CalculationNode {
    fn add_tags(&mut self, tag_list:Vec<String>){
        for item in tag_list {
            if !self.tags.contains(&item) {
                self.tags.push(item.clone());
            }
        }
    }
}

/// The main class that defines the whole data storage structure.
#[derive(Serialize, Deserialize, Default, Debug)]
struct JsonStorage {
    calculation_nodes: HashMap<String, CalculationNode>,
    data_nodes: HashMap<String, DataNode>,
}

/// Enum that wraps around datanodes and calculation_nodes
enum Node<'a> {
    Calculation(&'a CalculationNode),
    Data(&'a DataNode),
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
    fn add_calculation(&mut self, base_name: &String, command_string: & String ) {
        
        // validate input
        if self.calculation_nodes.contains_key(base_name) {
            panic!("Trying to add a calculation with a name that already exists. Aborting. Nothing being written to the database.")
        }


        // Extract inputs and outputs
        let input_re = Regex::new(r"input\((.*?)\)").expect("failed at creating a regular expression.");
        let output_re: Regex = Regex::new(r"output\((.*?)\)").expect("Failed at creating regulary expression."); // Match 'output(file)'
        
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
        

        // Check the final command
        // If the final command contains () - panic and crash. Most likely mispelled input
        if final_command.contains("(") | final_command.contains(")") {
            panic!("Found '(' or ')' in the final command - most likely mispelled 'input' or 'output'.")
        }

        
        let calculation_manager = CalculationManager{inputs: inputs.clone(), outputs: outputs.clone(), program: final_command};
        
        let calculation_node = CalculationNode{calculation: calculation_manager, copy:CopyManager::default(), git_hash: "".to_string(), tags:Vec::new()};
        println!("Adding to the database:");
        println!("{}", calculation_node);
        
        self.calculation_nodes.insert(base_name.to_string(), calculation_node);
        
        // Create all the data nodes.
        for input in inputs.clone() {
            let data_node = DataNode{save: false, tags: Vec::new(), copy: CopyManager::default()};
            self.data_nodes.insert(input, data_node);
        }

        for output in outputs.clone() {
            let data_node = DataNode{save: false, tags: Vec::new(), copy: CopyManager::default()};
            self.data_nodes.insert(output, data_node);
        }
        


    }

    /// Inspect a node for further information
    fn inspect(& self, name: &String){
        
        // check if it's a calculation node or a data node
        match self.get_node(name) {
            Ok(node) => {
                match node {
                    Node::Calculation(calculation_node) => {
                        println!("Calculation node:{}", name);
                        println!("{}", calculation_node);
                    }
                    Node::Data(data_node) =>  {println!("Data node: {}.", name);}
                }
            }
            Err(e) => panic!("{}",e .to_string())
        }

    }

    /// Try getting a node from a database. Could be any type of node
    /// NOTE: I don't want to return a copy, I want to return a view into the class so that I could modify it later
    /// Note this is only for refencing
    /// 
    /// Other options: 1) dynamic dispatch; 2) Common trait and generics (not sure if this would work, probably would have to know the result an compile time)
    /// 3) enum
    fn get_node(&self, name: &String) -> Result<Node, String>{

        let calculation_branch = self.calculation_nodes.contains_key(name);
        let data_branch = self.data_nodes.contains_key(name);

        if !calculation_branch && !data_branch {
            return Err("Node not found among calculation nodes or data nodes.".to_string())
        }

        if calculation_branch {
            let node = self.calculation_nodes.get(name).expect("Failed to find a calculation node.");
            let return_node = Node::Calculation(node);
            return Ok(return_node)

        }
        else {
            let node = self.data_nodes.get(name).expect("Failed to find the data node");
            let return_node = Node::Data(node);
            return Ok(return_node)
        }

    }


    /// Add tags to given nodes
    fn add_tags(&mut self, node_names: &Vec<String>, tag_list: &Vec<String>) -> Result<(), String> {


        for node_name in node_names {
            
            let calculation_branch = self.calculation_nodes.contains_key(node_name);
            let data_branch = self.data_nodes.contains_key(node_name);
    
            if !calculation_branch && !data_branch {
                return Err("Node not found among calculation nodes or data nodes.".to_string())
            }

            if calculation_branch {
                let node = self.calculation_nodes.get_mut(node_name).expect("Failed to find a calculation node.");
                for tag in tag_list {
                    if !node.tags.contains(tag){
                        node.tags.push(tag.clone());
                    }
                }
            }
            else {
                let node = self.data_nodes.get_mut(node_name).expect("Failed to find the data node");
                for tag in tag_list {
                    if !node.tags.contains(tag){
                        node.tags.push(tag.clone());
                    }
                }
            }
    
            
        }
        
        Ok(())

        
    }


    /// Remove tags from the database
    fn remove_tags(&mut self, node_names: &Vec<String>, tag_list: &Vec<String>)-> Result<(), String> {

        for node_name in node_names {
            
            let calculation_branch = self.calculation_nodes.contains_key(node_name);
            let data_branch = self.data_nodes.contains_key(node_name);
    
            if !calculation_branch && !data_branch {
                return Err("Node not found among calculation nodes or data nodes.".to_string())
            }

            if calculation_branch {
                let node = self.calculation_nodes.get_mut(node_name).expect("Failed to find a calculation node.");
                for tag in tag_list {
                   node.tags.retain(|x | x != tag);
                }
            }
            else {
                let node = self.data_nodes.get_mut(node_name).expect("Failed to find the data node");
                for tag in tag_list {
                    for tag in tag_list {
                        node.tags.retain(|x | x != tag);
                     }
                }
            }
    
            
        }
        
        Ok(())



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
    /// Create minimal database that can serve as a strarting point.
    Init,
    /// Read all the data and print it
    ReadData,
    /// Add a calculation to the database.
    AddCalculation {name: String, command : String},
    /// Inspect a node
    Inspect {name: String},

    /// add tags to given nodes
    AddTags {
        #[clap(long = "nodes", required = true)]
        nodes:Vec<String>, 
        #[clap(long = "tags", required = true)]
        tags: Vec<String>
    },

    /// Remove tags to given nodes
    RemoveTags {
        #[clap(long = "nodes", required = true)]
        nodes:Vec<String>, 
        #[clap(long = "tags", required = true)]
        tags: Vec<String>
    },

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
            let mut db = read_json_file("test.json").expect("Failed to read the database");
            db.add_calculation(&name, &command);
            db.write_database(&"test.json".to_string()).expect("failed to write the database.")
        }  
        Commands::Inspect {name} => {
            let mut db = read_json_file("test.json").expect("Failed to read the database");
            db.inspect(&name);
        }
        Commands::AddTags { nodes, tags } => {
            let mut db = read_json_file("test.json").expect("Failed to read the database");
            db.add_tags(&nodes, &tags).expect("Faile to add tags");
            db.write_database(&"test.json".to_string()).expect("failed to write the database.")
        }
        Commands::RemoveTags { nodes, tags } => {
            let mut db = read_json_file("test.json").expect("Failed to read the database");
            db.remove_tags(&nodes, &tags).expect("Faile to add tags");
            db.write_database(&"test.json".to_string()).expect("failed to write the database.")
        }

    }
}