use clap::{Parser,Subcommand};
use petgraph::data;
use petgraph::visit::EdgeRef;
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::{self, Write, Read};
use std::collections::BTreeMap;
use regex::Regex;
use std::fmt;
// Use the graphing tool
use petgraph::graph::{NodeIndex, DiGraph, UnGraph};
use petgraph::dot::{Dot, Config};
use petgraph::algo::has_path_connecting;
use std::time::{SystemTime, UNIX_EPOCH};
use std::path::{Path, PathBuf};
// Create a new database stucture for storing all the json data


const JSONDATABASE: &str  = ".graph/graph.json";
const CURRENTTAGS: &str  = ".graph/current_tags.json";

/// Manages inputs, outputs and the command to run
#[derive(Serialize, Deserialize, Default,Clone, Debug)]
struct CalculationManager {
    inputs: Vec<String>,
    outputs: Vec<String>,
    program: String
}


impl CalculationManager {
    /// Generate the command to run the calculation.
    fn get_full_program(& self, folder_base: &str) -> String {

        let relative_path = Path::new(folder_base);
        let mut final_command = self.program.clone();


        for (i, filename) in self.outputs.iter().enumerate() {
            let full_filename = relative_path.join(filename).to_string_lossy().to_string();
            final_command = final_command.replace(&format!("$output_{}", i), &full_filename);
        }

        for (i, filename) in self.inputs.iter().enumerate() {
            let full_filename = relative_path.join(filename).to_string_lossy().to_string();
            final_command = final_command.replace(&format!("$input_{}", i), &full_filename);
        }
        final_command

    }

    /// Generate a string that fills in most of the details for generating new things.
    /// Keep input the same; but strip the outputs
    fn get_program_template(& self) -> String {

        let mut final_command = self.program.clone();
        let re = Regex::new(r"^(\d+)(.+)").expect("Failed to compile regular expression.");

        for (i, filename) in self.inputs.iter().enumerate() {
            let full_filename = format!("input({})", filename);
            final_command = final_command.replace(&format!("$input_{}", i), &full_filename);
        }

        for (i, filename) in self.outputs.iter().enumerate() {
            
            let filename = match re.captures(filename) {
                Some(value) =>{
                    value
                    .get(2)
                    .expect("Failed to get the string. Something is wrong with string formating!")
                    .as_str()
                },
                None => panic!("Bad string format! Every string should begin with digits indicating unique timestamp.")
            };

            let full_filename = format!("output({})",filename);
            final_command = final_command.replace(&format!("$output_{}", i), &full_filename);
        }   

        final_command
    }


}


/// Manages copy history 
#[derive(Serialize, Deserialize, Default,Clone, Debug)]
struct CopyManager {
    name: String, // Name of the copy node
    origin: String // Name of the origin node
}

/// Describes a calculation node in a graph
#[derive(Serialize, Deserialize, Default,Clone, Debug)]
struct CalculationNode {
    git_hash: String,
    tags: Vec<String>, // For stornig things like the experiment or other thigs. 
    calculation: CalculationManager,
    copy: CopyManager,
}


#[derive(Serialize, Deserialize, Default,Clone, Debug)]
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
    calculation_nodes: BTreeMap<String, CalculationNode>,
    data_nodes: BTreeMap<String, DataNode>,
}

/// Enum that wraps around datanodes and calculation_nodes
enum Node<'a> {
    Calculation(&'a CalculationNode),
    Data(&'a DataNode),
}

#[derive(Serialize, Deserialize, Default, Debug)]
struct CurrentTags {
    tags:Vec<String>
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
    
    /// Merge two databases
    /// This overwrites the nodes if there are clashes. This would be used if want to add tags and then save the results
    /// TODO: Add different modes of addition - if there is a node with the same name being added, I could either overwrite or combine the tags.
    fn add_database(&mut self, other_db: &JsonStorage) {
        for (calc_name, calc_node) in other_db.calculation_nodes.iter() {
            self.calculation_nodes.insert(calc_name.clone(), calc_node.clone());
        }
    
        for (data_name, data_node) in other_db.data_nodes.iter() {
            self.data_nodes.insert(data_name.clone(), data_node.clone());
        }

    }

    /// Add a new calculation to the database
    fn add_calculation(&mut self, base_name: &String, command_string: & String ) {
        
        // validate input
        let base_name_formated = format_data_entry(base_name);

        if self.calculation_nodes.contains_key(&base_name_formated) {
            panic!("Trying to add a calculation with a name that already exists. Aborting. Nothing being written to the database.")
        }
        // ADD MORE VALIDATION. Make sure all inputs have time string attached to them!!!!!
        // If there is no number present, then add it automatically, that will simplify creation of new calculations.


        // Extract inputs and outputs
        let input_re = Regex::new(r"input\((.*?)\)").expect("failed at creating a regular expression.");
        let output_re: Regex = Regex::new(r"output\((.*?)\)").expect("Failed at creating regulary expression."); // Match 'output(file)'
        
        let mut inputs: Vec<String> = input_re
            .captures_iter(command_string)
            .map(|cap| cap[1].to_string()) // Get the file name without 'input()'
            .collect();

        let mut outputs: Vec<String> = output_re
            .captures_iter(command_string)
            .map(|cap| cap[1].to_string()) // Get the file name without 'input()'
            .collect();


        
        // Format the command string
        
        let mut final_command = command_string.clone();
        
        for (i, value) in inputs.iter().enumerate() {
            final_command = final_command.replace(&format!("input({})",value), &format!("$input_{}", i));
        }
        
        for (i, value) in outputs.iter().enumerate() {
            final_command = final_command.replace(&format!("output({})",value), &format!("$output_{}", i));
        }
        
        // Format the string
        for name in &mut inputs {
            *name = format_data_entry(name);
        }
        for name in &mut outputs {
            *name = format_data_entry(name);
        }

        // Check the final command
        // If the final command contains () - panic and crash. Most likely mispelled input
        if final_command.contains("(") | final_command.contains(")") {
            panic!("Found '(' or ')' in the final command - most likely mispelled 'input' or 'output'.")
        }

        
        let calculation_manager = CalculationManager{inputs: inputs.clone(), outputs: outputs.clone(), program: final_command};        
        let calculation_node = CalculationNode{calculation: calculation_manager, copy:CopyManager::default(), git_hash: "".to_string(), tags:Vec::new()};

        
        self.calculation_nodes.insert(base_name_formated.to_string(), calculation_node);
        
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
    fn inspect(& self, name: &String, data_folder: &String){
        
        // check if it's a calculation node or a data node
        match self.get_node(name) {
            Ok(node) => {
                match node {
                    Node::Calculation(calculation_node) => {
                        println!("Calculation node:{}", name);
                        println!("Tags: {:?}", calculation_node.tags);
                        println!("graphr new-calculation {} \"{}\"", get_calculation_basename(name).expect("Failed to capture"), calculation_node.calculation.get_program_template());
                        println!("{}", calculation_node.calculation.get_full_program(data_folder));
                    }
                    Node::Data(data_node) =>  {
                        println!("Tags: {:?}", data_node.tags);
                        println!("Data node: {}.", name);
                    }
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
    /// 3) enum; 4) Or maybe I could change the strucutre, where the hash map I story enums and not classes.
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

    /// Get nodes that contain the given substring
    fn get_similar_nodes(&self, name: &String) -> Vec<String>{

        let mut name_list: Vec<String> = Vec::new();

        for calc_name in self.calculation_nodes.keys() {
            if calc_name.contains(name) {
                name_list.push(calc_name.clone());
            }
        }

        for node_name in self.data_nodes.keys() {
            if node_name.contains(name) {
                name_list.push(node_name.clone());
            }
        }

        name_list

    }

    /// Add tags to given nodes
    fn add_tags(&mut self, tag_list: &Vec<String>) -> Result<(), String> {

        let node_names: Vec<String> = self.calculation_nodes.keys().cloned().collect();

        for node_name in node_names {
            let node = self.calculation_nodes.get_mut(&node_name).expect("Failed to find a calculation node.");
            
            for tag in tag_list {
                if !node.tags.contains(tag) {
                    node.tags.push(tag.clone());
                }
            }
        }

        let node_names: Vec<String> = self.data_nodes.keys().cloned().collect();
        for node_name in node_names {
            let node = self.data_nodes.get_mut(&node_name).expect("Failed to find a calculation node.");
            
            for tag in tag_list {
                if !node.tags.contains(tag) {
                    node.tags.push(tag.clone());
                }
            }
        }
      
        Ok(())

        
    }

    /// Set tags for the database. Overwrites the old ones
    fn set_tags(&mut self, tag_list: &Vec<String>) -> Result<(), String> {
        let node_names: Vec<String> = self.calculation_nodes.keys().cloned().collect();

        for node_name in node_names {
            let node = self.calculation_nodes.get_mut(&node_name).expect("Failed to find a calculation node.");            
            node.tags = tag_list.clone();

        }

        let node_names: Vec<String> = self.data_nodes.keys().cloned().collect();
        for node_name in node_names {
            let node = self.data_nodes.get_mut(&node_name).expect("Failed to find a calculation node.");
            node.tags = tag_list.clone();
        }
      
        Ok(())


    }


    /// Remove tags from the database
    fn remove_tags(&mut self, tag_list: &Vec<String>)-> Result<(), String> {


        let node_names: Vec<String> = self.calculation_nodes.keys().cloned().collect();

        // Then iterate over the collected names
        for node_name in node_names {
            let node = self.calculation_nodes.get_mut(&node_name).expect("Failed to find a calculation node.");
            
            for tag in tag_list {
                // Remove the tag if it exists
                if let Some(index) = node.tags.iter().position(|t| t == tag) {
                    node.tags.remove(index);
                }
            }
        }

        let node_names: Vec<String> = self.data_nodes.keys().cloned().collect();

        // Then iterate over the collected names
        for node_name in node_names {
            let node = self.data_nodes.get_mut(&node_name).expect("Failed to find a calculation node.");
            
            for tag in tag_list {
                // Remove the tag if it exists
                if let Some(index) = node.tags.iter().position(|t| t == tag) {
                    node.tags.remove(index);
                }
            }
        }

        Ok(())

    }

    /// Returns a filtered with nodes that only have a certain tag.
    fn filter_by_tags(& self, tags_to_include: &Vec<String>, tags_to_exclude : &Vec<String>) -> JsonStorage{

        // create an emtyp object
        let mut filtered_database = JsonStorage::default();

        // iterate through all the calculation_nodes
        for (node_name, node) in self.calculation_nodes.iter() {
            let mut overlap = false;
             // NOTE: this uses the simplest to implement algorithm: Could convert to a hashSet, or maybe sorting two-pointer approach
            for tag1 in &node.tags {
                for tag2 in tags_to_include {
                    if tag1 == tag2 {
                        overlap = true;
                    }
                }
            }

            for tag1 in &node.tags {
                for tag2 in tags_to_exclude {
                    if tag1 == tag2 {
                        overlap = false;
                    }
                }
            }

            if overlap {
                filtered_database.calculation_nodes.insert(node_name.to_string(), node.clone());
            }

        }

        for (node_name, node) in self.data_nodes.iter() {
            let mut overlap = false;
             // NOTE: this uses the simplest to implement algorithm: Could convert to a hashSet, or maybe sorting two-pointer approach
            for tag1 in &node.tags {
                for tag2 in tags_to_include {
                    if tag1 == tag2 {
                        overlap = true;
                    }
                }
            }
            for tag1 in &node.tags {
                for tag2 in tags_to_exclude {
                    if tag1 == tag2 {
                        overlap = false;
                    }
                }
            }

            if overlap {
                filtered_database.data_nodes.insert(node_name.to_string(), node.clone());
            }
        }

        // iterate through all the data nodes
        filtered_database

    }

    /// Covert database to a DiGraph (could be a filtered database) to a graph representation for selection of the graph in other ways and plotting too.
    fn generate_digraph(& self) -> (DiGraph::<&str, &str>, BTreeMap<String, NodeIndex>){
        
        let mut graph = DiGraph::<&str, &str>::new(); // initialize the final graph
        let mut graph_nodes:  BTreeMap<String, NodeIndex> = BTreeMap::new(); // node storage thing
        let mut edges: Vec<(NodeIndex,NodeIndex)> = Vec::new(); 

        // Create nodes for the graph
        for calc_name in self.calculation_nodes.keys() {
            let gn = graph.add_node(&calc_name);
            graph_nodes.insert(calc_name.clone(), gn);
        }
        for data_name in self.data_nodes.keys() {
            let gn = graph.add_node(&data_name);
            graph_nodes.insert(data_name.clone(), gn);
        }


        // Add edges to the graph

        for (calc_name, calc_node) in self.calculation_nodes.iter() {
            for inp in &calc_node.calculation.inputs {
                match graph_nodes.get(inp) {
                    Some(value) => {edges.push((*value, *graph_nodes.get(calc_name).expect(&format!("input {} found for {} calculation", &inp, &calc_name))))}
                    None => {}
                }

            }

            for outp in &calc_node.calculation.outputs {
                match graph_nodes.get(outp) {
                    Some(value) => {edges.push((*graph_nodes.get(calc_name).expect(&format!("input {} found for {} calculation", &outp, &calc_name)), *value))},
                    None => {}
                } 
                
            }
        }

        graph.extend_with_edges(&edges);
        return (graph, graph_nodes)

    }

    /// Similar to the previous one, but generates undirected graph.
    fn generate_ungraph(& self) -> (UnGraph::<&str, ()>, BTreeMap<String, NodeIndex>){
        
        let mut graph = UnGraph::<&str, ()>::new_undirected(); // initialize the final graph
        let mut graph_nodes:  BTreeMap<String, NodeIndex> = BTreeMap::new(); // node storage thing
        let mut edges: Vec<(NodeIndex,NodeIndex)> = Vec::new(); 

        // Create nodes for the graph
        for calc_name in self.calculation_nodes.keys() {
            let gn = graph.add_node(&calc_name);
            graph_nodes.insert(calc_name.clone(), gn);
        }
        for data_name in self.data_nodes.keys() {
            let gn = graph.add_node(&data_name);
            graph_nodes.insert(data_name.clone(), gn);
        }


        // Add edges to the graph

        for (calc_name, calc_node) in self.calculation_nodes.iter() {
            for inp in &calc_node.calculation.inputs {
                // It could be a case that I have filtered some outputs or inputs. In this case do nothing
                match graph_nodes.get(inp) {
                    Some(value) => {edges.push((*value, *graph_nodes.get(calc_name).expect("failed")))},
                    None => {}
                }
            }

            for outp in &calc_node.calculation.outputs {
                match graph_nodes.get(calc_name) {
                    Some(value) => {edges.push((*graph_nodes.get(calc_name).expect("failed"), *value))},
                    None => {}
                }

            }
        }

        graph.extend_with_edges(&edges);
        return (graph, graph_nodes)

    }

    /// Given a name of the node, finds all connected nodes and returns a new, smaller graph
    fn select_disconected_branch(&self, name: &String) -> DiGraph<String, ()> {
        let mut new_graph: DiGraph<String, ()> = DiGraph::new();
        let (current_graph, current_node_name_map) = self.generate_ungraph();
        let origin_node = current_node_name_map.get(name).expect("Failed to find node in the database!").clone();
        
        // Create a mapping between original node indices and new node indices
        let mut node_mapping: BTreeMap<NodeIndex, NodeIndex> = BTreeMap::new();
        
        // First, add all connected nodes to the new graph
        for (node_name, node_index) in current_node_name_map.iter() {
            if has_path_connecting(&current_graph, *node_index, origin_node, None) {
                // Create a new node with an owned String
                let new_idx = new_graph.add_node(node_name.clone());
                node_mapping.insert(*node_index, new_idx);
            }
        }
        
        // Now add the edges between the nodes in the new graph
        for (node_name, node_index) in current_node_name_map.iter() {
            if let Some(&new_idx) = node_mapping.get(node_index) {
                if self.calculation_nodes.contains_key(node_name) {
                    let calc_node = self.calculation_nodes.get(node_name).expect("failed to get the node.");
                    
                    // Add edges for inputs
                    for inp in &calc_node.calculation.inputs {
                        if let Some(&input_node) = current_node_name_map.get(inp) {
                            if let Some(&new_input_idx) = node_mapping.get(&input_node) {
                                new_graph.add_edge(new_input_idx, new_idx, ());
                            }
                        }
                    }
                    
                    // Add edges for outputs
                    for outp in &calc_node.calculation.outputs {
                        if let Some(&output_node) = current_node_name_map.get(outp) {
                            if let Some(&new_output_idx) = node_mapping.get(&output_node) {
                                new_graph.add_edge(new_idx, new_output_idx, ());
                            }
                        }
                    }
                }
            }
        }
        
        new_graph
    }

    /// Select all nodes that produce a certain file. Select the whole history and return a new graph
    fn select_node_history(&self, name: &String) -> DiGraph<String, ()> {

        let mut new_graph: DiGraph<String, ()> = DiGraph::new();
        let (current_graph, current_node_name_map) = self.generate_digraph();
        let origin_node = current_node_name_map.get(name).expect("Failed to find node in the database!").clone();
        
        // Create a mapping between original node indices and new node indices
        let mut node_mapping: BTreeMap<NodeIndex, NodeIndex> = BTreeMap::new();
        
        // Find all calculation nodes that needed to produce the calculation.
        for (node_name, node_index) in current_node_name_map.iter() {
            if self.calculation_nodes.contains_key(node_name) && has_path_connecting(&current_graph, *node_index, origin_node, None) {
                // Create a new node with an owned String
                let new_idx = new_graph.add_node(node_name.clone());
                node_mapping.insert(*node_index, new_idx);

                // Instert calculation nodes inputs and outputs to the mapping
                for input_name in &self.calculation_nodes.get(node_name).expect("failed to find a calculation node").calculation.inputs {
                    let new_idx = new_graph.add_node(input_name.clone());
                    node_mapping.insert(*current_node_name_map.get(input_name).expect("failed to get a node."), new_idx);
                }
                for output_name in &self.calculation_nodes.get(node_name).expect("failed to find a calculation node").calculation.outputs {
                    let new_idx = new_graph.add_node(output_name.clone());
                    node_mapping.insert(*current_node_name_map.get(output_name).expect("failed to get a node."), new_idx);
                }
            }
        }



        
        // Now add the edges between the nodes in the new graph
        for (node_name, node_index) in current_node_name_map.iter() {
            if let Some(&new_idx) = node_mapping.get(node_index) {
                if self.calculation_nodes.contains_key(node_name) {
                    let calc_node = self.calculation_nodes.get(node_name).expect("failed to get the node.");
                    
                    // Add edges for inputs
                    for inp in &calc_node.calculation.inputs {
                        if let Some(&input_node) = current_node_name_map.get(inp) {
                            if let Some(&new_input_idx) = node_mapping.get(&input_node) {
                                new_graph.add_edge(new_input_idx, new_idx, ());
                            }
                        }
                    }
                    
                    // Add edges for outputs
                    for outp in &calc_node.calculation.outputs {
                        if let Some(&output_node) = current_node_name_map.get(outp) {
                            if let Some(&new_output_idx) = node_mapping.get(&output_node) {
                                new_graph.add_edge(new_idx, new_output_idx, ());
                            }
                        }
                    }
                }
            }
        }

        new_graph
    }

    /// Copies all outgoing nodes for a given database and attaches the same nodes (copied) to the destination
    fn select_node_future(& self, name : &String) -> DiGraph<String, ()> {

        let mut new_graph: DiGraph<String, ()> = DiGraph::new();
        let  (mut current_graph, current_node_name_map) = self.generate_digraph();

        current_graph.reverse();

        let origin_node = current_node_name_map.get(name).expect("Failed to find node in the database!").clone();
        
        // Create a mapping between original node indices and new node indices
        let mut node_mapping: BTreeMap<NodeIndex, NodeIndex> = BTreeMap::new();
        
        // Find all calculation nodes that needed to produce the calculation.
        for (node_name, node_index) in current_node_name_map.iter() {
            if self.calculation_nodes.contains_key(node_name) && has_path_connecting(&current_graph, *node_index, origin_node, None) {
                // Create a new node with an owned String
                let new_idx = new_graph.add_node(node_name.clone());
                node_mapping.insert(*node_index, new_idx);

                // Instert calculation nodes inputs and outputs to the mapping
                for input_name in &self.calculation_nodes.get(node_name).expect("failed to find a calculation node").calculation.inputs {
                    let new_idx = new_graph.add_node(input_name.clone());
                    node_mapping.insert(*current_node_name_map.get(input_name).expect("failed to get a node."), new_idx);
                }
                for output_name in &self.calculation_nodes.get(node_name).expect("failed to find a calculation node").calculation.outputs {
                    let new_idx = new_graph.add_node(output_name.clone());
                    node_mapping.insert(*current_node_name_map.get(output_name).expect("failed to get a node."), new_idx);
                }
            }
        }



        
        // Now add the edges between the nodes in the new graph
        for (node_name, node_index) in current_node_name_map.iter() {
            if let Some(&new_idx) = node_mapping.get(node_index) {
                if self.calculation_nodes.contains_key(node_name) {
                    let calc_node = self.calculation_nodes.get(node_name).expect("failed to get the node.");
                    
                    // Add edges for inputs
                    for inp in &calc_node.calculation.inputs {
                        if let Some(&input_node) = current_node_name_map.get(inp) {
                            if let Some(&new_input_idx) = node_mapping.get(&input_node) {
                                new_graph.add_edge(new_input_idx, new_idx, ());
                            }
                        }
                    }
                    
                    // Add edges for outputs
                    for outp in &calc_node.calculation.outputs {
                        if let Some(&output_node) = current_node_name_map.get(outp) {
                            if let Some(&new_output_idx) = node_mapping.get(&output_node) {
                                new_graph.add_edge(new_idx, new_output_idx, ());
                            }
                        }
                    }
                }
            }
        }

        new_graph.reverse();
        new_graph
    }
    /// Convert Graph to database object within the current database context.
    fn digraph_to_database(&self, graph: &DiGraph<String, ()>) -> JsonStorage {

        let mut calculation_nodes: BTreeMap<String, CalculationNode> = BTreeMap::new();
        let mut data_nodes: BTreeMap<String, DataNode> = BTreeMap::new();

        for node_id in graph.node_indices() {
            let node_name = graph.node_weight(node_id).expect("Failed to get a node name");
            
            // handle the calculation node 
            if self.calculation_nodes.contains_key(node_name) {
                calculation_nodes.insert(node_name.clone(), self.calculation_nodes.get(node_name).expect("failed").clone());
                continue
            }   
            if self.data_nodes.contains_key(node_name) {
                data_nodes.insert(node_name.clone(), self.data_nodes.get(node_name).expect("failed").clone());
                continue
            }
        }

        JsonStorage {calculation_nodes : calculation_nodes, data_nodes : data_nodes}
    }

    /// Select nodes by the given name
    fn select_by_name(&self, name_list: &Vec<String>) -> JsonStorage{

        let mut calculation_nodes: BTreeMap<String, CalculationNode> = BTreeMap::new();
        let mut data_nodes: BTreeMap<String, DataNode> = BTreeMap::new();

        for node_name in name_list {
            if self.calculation_nodes.contains_key(node_name) {
                calculation_nodes.insert(node_name.clone(), self.calculation_nodes.get(node_name).expect("failed to get a key").clone());
            }
        }

        for node_name in name_list {
            if self.data_nodes.contains_key(node_name) {
                data_nodes.insert(node_name.clone(), self.data_nodes.get(node_name).expect("failed to get a key").clone());
            }
        }

        JsonStorage {calculation_nodes: calculation_nodes, data_nodes : data_nodes}


    }

    /// Creates new calculations by copying the current database. (This should be used in conjunction with selection operators.)
    /// It keeps all the old tags and configurations of the old nodes. The structure should be passed to other commands to change those.
    /// reattchements - Should define all loose ends (for example inputs that are not present in the copy, but a calculation node needs it.)
    /// if an input node is not in the reattachements and not in the provided database - then a new node is created, if it's an output, then it creates a new node with no tags. If needed tags can always be added again.
    fn copy_database(& self, reattachments: &Vec<[String; 2]> ) -> JsonStorage {



        let mut new_data_nodes: BTreeMap<String, DataNode> = BTreeMap::new();
        let mut new_calc_nodes: BTreeMap<String, CalculationNode> = BTreeMap::new();
        let mut rename_map: BTreeMap<String, String> = BTreeMap::new();

        for attechement in reattachments {
            rename_map.insert(attechement[0].clone(), attechement[1].clone());
        }

        // add reattachements to the rename_map

        let re = Regex::new(r"^\d+").unwrap();
        
        //----------------------------
        // Create the rename map
        //----------------------------
        // Go through all the data nodes.
        for (node_name, node_obj) in self.data_nodes.iter() {
            // current time
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Failed to get current system time.")
                .as_nanos();
            // generate new key by changing the time stamp
            let new_name = re.replace(node_name, now.to_string()).to_string();

            rename_map.insert(node_name.clone(), new_name.clone());
        }
        
        // Go through all calculation nodes
        for (calc_name, calc_obj) in self.calculation_nodes.iter() {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Failed to get current system time.")
                .as_nanos();
            let new_name = re.replace(calc_name, now.to_string()).to_string();
            rename_map.insert(calc_name.clone(), new_name.clone());
        }

        // Overwrite the manual
        for attechement in reattachments {
            rename_map.insert(attechement[0].clone(), attechement[1].clone());
        }

        //----------------------------
        // Create the new database object
        //----------------------------

        // Create new Data nodes
        for (node_name, node_obj) in self.data_nodes.iter() {
            new_data_nodes.insert(rename_map.get(node_name).expect("failed").clone(), node_obj.clone());

        }

        // Create new caluclation nodes
        for (calc_name, calc_obj) in self.calculation_nodes.iter() {

            let mut new_calc_node = calc_obj.clone();

            // Update inputs with new names
            let mut updated_inputs = Vec::new();
            for inp in &new_calc_node.calculation.inputs {
                
                let new_inp = match rename_map.get(inp) {
                    Some(value) => value,
                    None => {
                        // Node is not in the database or extra renames. Use the same name
                        inp
                    }
                };

                updated_inputs.push(new_inp.clone());
            }
            new_calc_node.calculation.inputs = updated_inputs;

            // Update outputs with new names
            let mut updated_outputs = Vec::new();
            for outp in &new_calc_node.calculation.outputs {

                let new_outp = match rename_map.get(outp) {
                    Some(value) => value,
                    None => outp
                };

                updated_outputs.push(new_outp.clone());
            }
            new_calc_node.calculation.outputs = updated_outputs;

            new_calc_nodes.insert(rename_map.get(calc_name).expect("failed").clone(), new_calc_node);
        }




        
        let new_db = JsonStorage {
            calculation_nodes: new_calc_nodes,
            data_nodes: new_data_nodes,
        };

        new_db
    }

    /// Deletes nodes from the database.
    /// For the calculation nodes also delete all outputs
    fn delete(&mut self, node_names: &Vec<String>) {
        
        let mut remove_calculation_nodes : Vec<&String> = Vec::new(); 

        for node_name in node_names {
            if self.calculation_nodes.contains_key(node_name) {
                remove_calculation_nodes.push(node_name)
            }
        }
        
        for remove_node in remove_calculation_nodes {
            self.calculation_nodes.remove(remove_node);
        }


        let mut remove_data_nodes : Vec<&String> = Vec::new(); 

        for node_name in node_names {
            if self.data_nodes.contains_key(node_name) {
                remove_data_nodes.push(node_name)
            }
        }
        
        for remove_node in remove_data_nodes {
            self.data_nodes.remove(remove_node);
        }


    }



}


/// Formats a string to a format compatable for the database
fn format_data_entry(name: &String) -> String {

    // Add the current time to the input if it does not exist.
    let re = Regex::new(r"^\d{16}").unwrap();

    // correct branch
    if re.is_match(name) {

        return name.clone()
    }
    else {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Failed to get current system time.")
            .as_nanos();
        

        let mut new_name = name.clone();
        new_name.insert_str(0, &now.to_string());
        return new_name
        }



}





fn read_json_file(filename: &str) -> std::io::Result<JsonStorage> {
    let mut file = File::open(filename)?; // Open the file
    let mut contents = String::new();
    file.read_to_string(&mut contents)?; // Read file into a string
    let db: JsonStorage  = serde_json::from_str(&contents)?; // Deserialize JSON
    Ok(db)
}

fn read_current_file(filename: &str) -> std::io::Result<CurrentTags> {
    let mut file = File::open(filename)?; // Open the file
    let mut contents = String::new();
    file.read_to_string(&mut contents)?; // Read file into a string
    let db: CurrentTags = serde_json::from_str(&contents)?; // Deserialize JSON
    Ok(db)
}


/// handles whether the database comes from stdin or as the last argument named 'database'.
fn get_database_input(database: &Option<String>) -> JsonStorage{

    let database_json_string = match database {
        Some(data) => {data.clone()}
        None => {                
            let mut buffer = String::new();
            io::stdin().read_to_string(&mut buffer).expect("Failed to read from stdin");
            buffer
        }
    };
    let db: JsonStorage = serde_json::from_str(&database_json_string).expect("Failed converting Json to the database object. Aborting.");
    return db
}

fn write_database_to_stream(database: &JsonStorage){

    let write_string = serde_json::to_string(database).expect("Failed to seriazile the database for printing.");

    // ------
    let stdout = io::stdout();
    let mut handle = stdout.lock();

    // Try writing to stdout
    if let Err(e) = writeln!(handle, "{}", write_string) {
        if e.kind() == io::ErrorKind::BrokenPipe {
            // Exit gracefully if the pipe is closed early
            std::process::exit(0);
        } else {
            eprintln!("Failed to write to stdout: {}", e);
            std::process::exit(1);
        }
    }

}

/// Given a calculation name extarcts the basename (removes initial digits)
fn get_calculation_basename(name : &String) -> Result<String, &str> {

    let re = Regex::new(r"^(\d+)(.+)").expect("Failed to compile regular expression.");

    match re.captures(&name) {
        Some(value) => {
            match value.get(2) {
                Some(value) => {
                    return Ok(value.as_str().to_string())
                }
                None => {return Err("could not capture the string. Wrong formatting!")}
            }
        }
        None => {return Err("Wrong string format!")}
    }

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
    /// Initialize the databse
    Init,

    /// Get the database and inject that into the stdout.
    Get,

    /// Get Nodes that have a given substring
    GetSimilar {
        name:String,
        database: Option<String>
    },

    /// Add a calculation to the database.
    NewCalculation {
        name: String,
        command : String,
        },
    /// Inspect a node
    Inspect {name: String, 
        #[clap(default_value = "data")] 
        datafolder: String
        },
    /// add tags to given nodes
    AddTag {

        #[clap(long = "tag", required = true)]
        tag: Vec<String>,
        /// Database in the string format
        database: Option<String>
    },
    /// Set all the tags for the given (sub)database.
    SetTags {

        #[clap(long = "tag", required = true)]
        tags: Vec<String>,
        /// Database in the string format
        database: Option<String>
    },
    /// Remove tags to given nodes
    RemoveTag {

        #[clap(long = "tag", required = true)]
        tag: Vec<String>,
        /// Database in the string format
        database: Option<String>
    },
    /// Select nodes by tag
    SelectTag {
        /// Tags to include
        #[clap(long = "tag", default_values_t = Vec::<String>::new())]
        tags:Vec<String>,
        /// Tags to exclude
        #[clap(long = "notag",default_values_t = Vec::<String>::new())]
        notags:Vec<String>,

        /// Database in the string format
        database: Option<String>
    },
    SelectSubbranch { name: String, database: Option<String>},

    /// Select all nodes that come to produce a certain node.
    SelectHistory {name:String, database:Option<String>},

    /// Select a part of the database by name
    SelectName {        
        #[clap(long = "name", required = true)]
        names:Vec<String>,

        /// Database in the string format
        database: Option<String>},

    /// Visualize the graph
    Show {
        database: Option<String>
    },
    /// Rename nodes
    Copy {
        #[arg(
            long = "attach", 
            num_args = 2,  // Requires exactly 2 values per occurrence
            help = "Specify a pair of names to attach (requires exactly 2 names)",
        )]
        attach: Option<Vec<String>>,

        database: Option<String>
    },

    /// Adds given stream from the command line to the actual database.
    Add {
        /// Database passed from the coomand line
        database: Option<String>
    },
    /// delete named nodes from the database
    Delete {
        #[clap(long = "name", required = true)]
        names:Vec<String>,
    },

    /// Find all outgoing nodes from one node and create a copy on some other node
    /// Used to quickly create calculations for new modifications
    SelectFuture {
        name:String,
        /// Database in the string format
        database: Option<String>
    }

}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Init  => {
            let calculation_manager = CalculationManager{inputs:vec!["input_1".to_string()],
                    outputs: vec!["output_2".to_string()],
                    program: "python3 input_1 output_2".to_string()};

            let copy_manager = CopyManager {name: "test".to_string(), origin: "another_test".to_string()};
            
            let mut calculation_nodes = BTreeMap::new();
            calculation_nodes.insert("test".to_string(), CalculationNode{git_hash: "".to_string(), tags: Vec::new(), calculation: calculation_manager, copy: copy_manager});
            let mut data_nodes = BTreeMap::new();
            data_nodes.insert("test_data".to_string(), DataNode{save:true, tags:Vec::new(), copy: CopyManager::default()});                                                
            let mut default_struct = JsonStorage{calculation_nodes: calculation_nodes, data_nodes: data_nodes};
            default_struct.write_database(&JSONDATABASE.to_string());
        }
        Commands::Get  => {
            let db = read_json_file(JSONDATABASE).expect("Failed to read the database");
            write_database_to_stream(&db);
        }
        Commands::GetSimilar {name, database} => {
            let db = get_database_input(database);
            let name_list = db.get_similar_nodes(&name);

            for name in name_list {
                println!("{}",name);
            }

        }
        Commands::NewCalculation {name, command} => {
            let mut db =  JsonStorage::default();
            db.add_calculation(&name, &command);
            write_database_to_stream(&db);
        }  
        Commands::Inspect {name, datafolder} => {
            let mut db = read_json_file(JSONDATABASE).expect("Failed to read the database");
            db.inspect(&name, &datafolder);
        }
        Commands::AddTag { tag, database } => {
            let mut db = get_database_input(database);
            db.add_tags( &tag).expect("Faile to add tags");
            write_database_to_stream(&db);
        }
        Commands::SetTags { tags, database } => {
            let mut db = get_database_input(database);
            db.set_tags( &tags).expect("Failed to set tags");
            write_database_to_stream(&db);

        }
        Commands::RemoveTag {tag, database } => {
            let mut db = get_database_input(database);
            db.remove_tags(&tag).expect("Faile to add tags");
            write_database_to_stream(&db);
        }
        Commands::SelectTag { tags, notags, database } => {
            
            let db = get_database_input(database);
            let new_db = db.filter_by_tags(tags, notags);
            write_database_to_stream(&new_db);
        }
        Commands::SelectSubbranch { name , database} => {
            let db = get_database_input(database);
            let graph = db.select_disconected_branch(name);
            let new_db = db.digraph_to_database(&graph);
            write_database_to_stream(&new_db);
        }
        Commands::SelectHistory { name, database } => {
            let db = get_database_input(database);
            let graph = db.select_node_history(name);
            let new_db = db.digraph_to_database(&graph);
            write_database_to_stream(&new_db);

        }
        Commands::Show { database } => {
            
            // handle the cases when the input is passed directly and when it could by piped.
            let db = get_database_input(database);
            let (graph, _graph_nodes) = db.generate_digraph();
            println!("{}", Dot::with_config(&graph, &[Config::EdgeNoLabel]));

        }
        Commands::Copy {attach, database} => {

        let attach_parsed = match attach {
            Some(value) => {
                // Filter to only include complete pairs and convert each chunk to a tuple
                value.chunks(2)
                    .map(|chunk| [chunk.get(0).expect("failed to get second value for the chunk.").clone(), chunk.get(1).expect("failed to get second value for the chunk.").clone()])
                    .collect::<Vec<[String;2]>>()
            }
            None => {
                Vec::new()
            }
        };

            let db = get_database_input(database);
            let copied_db = db.copy_database(&attach_parsed);
            write_database_to_stream(&copied_db);

        }
        Commands::Add {database} =>{
            let mut db = read_json_file(JSONDATABASE).expect("Failed to read the database");
            let db_std = get_database_input(database);
 
            // combine
            db.add_database(&db_std);
            db.write_database(JSONDATABASE);

            write_database_to_stream(&db_std);

        }
        Commands::Delete { names } => {
            let mut db = read_json_file(JSONDATABASE).expect("Failed to read the database");
            db.delete(names);
            db.write_database(JSONDATABASE);
        }
        Commands::SelectName { names, database } => {
            let db = get_database_input(database);
            let copied_db = db.select_by_name(names);
            write_database_to_stream(&copied_db);
        }
        Commands::SelectFuture { name, database } => {

            let db = get_database_input(database);
            let graph = db.select_node_future(name);
            let new_db = db.digraph_to_database(&graph);
            write_database_to_stream(&new_db);
        }
    }
}