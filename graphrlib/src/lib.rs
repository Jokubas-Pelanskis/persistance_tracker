/*
DNode - Only contains abstract calculations. real data are only described by location 

*/
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use pyo3::prelude::*;
use pyo3::types::*;
use pyo3::wrap_pyfunction;
use pyo3::types::PyType;
use serde::{Serialize, Deserialize};
use regex::Regex;
use std::time::{SystemTime, UNIX_EPOCH};
use std::fmt;
use std::collections::{HashSet, VecDeque};
use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use serde_json::{Value, Map};
use std::io::Write;
use petgraph::graph::{NodeIndex, DiGraph, UnGraph};
use petgraph::Direction;
use petgraph::dot::{Dot, Config};
use petgraph::algo::has_path_connecting;
use petgraph::visit::Topo;
use petgraph::visit::Walker;
use pyo3::types::PyDict;



/// Set types for interacting with the database
type IdCTemplate = String;
type IdDTemplate = String;
type IdC = String;
type IdD = String;
type IdNode = String; // Id of a general node (could be IdC or IdD)
type IdNodeTemplate = String;
type IdTemplate = String;




/// Describe Abstract Data and Calculation nodes
#[pyclass]
#[derive(Serialize, Deserialize, Default,Clone, Debug,PartialEq, Eq)]
pub struct DNodeTemplate {
    id: IdDTemplate
}


#[pyclass]
#[derive(Serialize, Deserialize, Default,Clone, Debug, PartialEq, Eq)]
pub struct CNodeTemplate {
    pub id: IdCTemplate,
    pub command: String,
    pub incoming: Vec<IdDTemplate>,
    pub outcoming: Vec<IdDTemplate>,
    pub extra: BTreeMap<String, ExtraData> // Extra data that can be passed to the node
}

#[pyclass]
#[derive(Clone)]
enum NodeTemplate {
    Calculation(CNodeTemplate),
    Data(DNodeTemplate),
}



/// Describes abstract calculations.
#[pyclass]
#[derive(Clone,serde::Serialize,Deserialize,Debug)]
pub struct DatabaseTemplate {
    cnodes: BTreeMap<IdCTemplate, CNodeTemplate>, // Store all calculation nodes
    dnodes: BTreeMap<IdDTemplate, DNodeTemplate>, // Store all data nodes
}

/// Describes implementations and actual calculations
#[pyclass]
#[derive(Clone,serde::Serialize,Deserialize)]
pub struct Database {

    template: DatabaseTemplate,
    // template: DatabaseTemplate, // Store the template
    cnodes: BTreeMap<IdC, CNode>, // Store all calculation nodes
    dnodes: BTreeMap<IdD, DNode>, // Store all data nodes

}



/// describe imlementations of nodes (These will have names with time stamps)
#[pyclass]
#[derive(Serialize, Deserialize, Default,Clone, Debug)]
pub struct DNode {
    #[pyo3(get)]
    pub id: IdD,
    #[pyo3(get)]
    pub template: IdDTemplate


}

/// Describes an abstract calculation node
#[pyclass]
#[derive(Serialize, Deserialize, Default,Clone, Debug)]
pub struct CNode{
    #[pyo3(get)]
    pub id: IdC,
    #[pyo3(get)]
    pub template: IdCTemplate,
    #[pyo3(get)]
    pub incoming: Vec<IdD>,
    #[pyo3(get)]
    pub outcoming: Vec<IdD>,
    #[pyo3(get)]
    pub extra: BTreeMap<String, ExtraData> // Extra data that can be passed to the node
}

#[pyclass]
#[derive(Clone)]
enum Node {
    Calculation(CNode),
    Data(DNode),
}


/// Extra data that can be passed to templates or nodes
/// This data can be used to modify the behaviour and if anything extra needs to be attached
/// First value is looked up in the instance and if not found then the default value from the template is used.
#[pyclass]
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
enum ExtraData {
    Int(i32),
    String(String),
    Bool(bool),
}



#[pymethods]
impl Node {

    /// Generate an id for a calculation node.
    #[staticmethod]
    fn generate_id() -> IdC{
        let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to get current system time.")
        .as_nanos()
        .to_string();
        now
    }


    #[getter]
    fn id(&self) -> String {
        match self {
            Node::Calculation(a) => a.id.clone(),
            Node::Data(a) => a.id.clone()
        }
    }

    // get all the outcoming nodes if it's a calculation node
    #[getter]
    fn outcoming(&self) -> Vec<String> {
        match self {
            Node::Calculation(a) => a.outcoming.clone(),
            Node::Data(a) => panic!("Data nodes do not have outcoming or incoming data assosiated with it. Provide Calculation node.")
        }
    }

    }


    // fn get_label(&self) -> String {
    //     match self {
    //         Node::Calculation(a) => a.get_label(),
    //         Node::Data(a) => a.get_label()
    //     }
    // }

    // fn get_id(&self) -> String {
    //     match self {
    //         Node::Calculation(a) => a.id.clone(),
    //         Node::Data(a) => a.id.clone()
    //     }
    // }




#[pymethods]
impl DNodeTemplate {
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("DNodeTemplate(id={})", self.id))
    }

    /// Format the string so that it could be interpreted by calculation creation 
    /// algorithm
    fn __format__(&self, spec: &str) -> PyResult<String> {
        let formatted = match spec {
            _ => format!("{}", self.id),
        };
        Ok(formatted)
    }
}


#[pymethods]
impl CNodeTemplate {
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("DNodeTemplate(id={};\ninput={:?},\noutput={:?})", self.id,self.incoming,self.outcoming ))
    }

}



impl CNodeTemplate {

    /// Parses a command to the desirable format
    /// command has inputs marked with input(<filename>) and output with output(<filename>)
    fn parse_command(command: String) -> (String, Vec<IdDTemplate>, Vec<IdDTemplate>) {
        
        let re = Regex::new(r"(input|output)\(([^)]+)\)").expect("Failed to compile input regex.");

        let mut inputs = Vec::new();
        let mut outputs = Vec::new();
        let mut input_counter = 0;
        let mut output_counter = 0;
        // Replace input(<filename>) with $i, where i is an integer enumerating all input

        let output = re.replace_all(&command, |caps: &regex::Captures| {
            let kind = &caps[1];     // "input" or "output"
            let value = &caps[2];    // the part inside parentheses
    
            match kind {
                "input" => {
                    inputs.push(value.to_string());
                    let replacement = format!("$i_{}", input_counter);
                    input_counter +=1;
                    replacement
                    },
                "output" => {
                    outputs.push(value.to_string());
                    let replacement = format!("$o_{}", output_counter);
                    output_counter +=1;
                    replacement

                },
                _ => {panic!("Could not parse the command correctly")}
            }
        });

        (output.to_string(), inputs, outputs)

    }
}



#[pymethods]
impl DatabaseTemplate {

    #[new]
    pub fn new() -> Self {
        DatabaseTemplate {
            cnodes: BTreeMap::new(),
            dnodes: BTreeMap::new(),
        }

    }

    fn __str__(&self) -> PyResult<String> {

        let cnodes = format!("{:?}", self.cnodes);
        let dnodes = format!("{:?}", self.dnodes);
        Ok(format!("DatabaseTemplate(cnodes={};\ndnodes={})",cnodes, dnodes  ))
    }




    /// Register data node
    pub fn register_dnode(&mut self, name: String) -> DNodeTemplate {

        let dnode = DNodeTemplate {id: name.clone()};
        self.dnodes.insert(name, dnode.clone());
        dnode
    }


    pub fn create_calculation_node(&self, name:String, command: String) -> CNodeTemplate {
        let values = CNodeTemplate::parse_command(command);

        let cnode = CNodeTemplate {
            id: name.clone(),
            incoming: values.1,
            outcoming: values.2,
            command: values.0,
            extra: BTreeMap::new(), // Extra data that can be passed to the node
        };
        cnode
    }

    /// Register calculation node
    /// If a key already exists, then comprate the value. If the values are the same do nothing,
    /// If they are different then crash, otherwise, overwrite flag is passed
    pub fn register_cnode(&mut self, name: String, command: String) -> CNodeTemplate {
        let cnode = self.create_calculation_node(name.clone(), command);

        self.cnodes.insert(name, cnode.clone());
        cnode

    }

    /// Return the database in DOT format
    pub fn as_dot(&self) -> String {
        let (graph,mapping) = self.generate_digraph();
        format!("{}", Dot::with_config(&graph, &[Config::EdgeNoLabel]))
    }


    /// get a node from a template
    pub fn get(&self, name: String) -> NodeTemplate {
        unimplemented!();
    }

    /// Create an implementation of a given template.
    pub fn create_calculation(&self, leafs: BTreeMap<String, String>) -> Database {
        
        // Need to go through all data and calculation nodes and generate actual calculations.

        // Go through all Data nodes

        let mut new_dnodes  = BTreeMap::new();
        let mut new_cnodes = BTreeMap::new();
        let mut dnode_mapping = BTreeMap::new(); // for fully mapping data nodes
        
        // Check if all root nodes have specified names
        // This is needed due to the imposed workflow.

        let root_nodes = self.find_root_nodes();
        let mut correct_input: bool = true;
        let mut error_message = String::from("");
        for rn in root_nodes {
            correct_input = leafs.contains_key(&rn);
            if !correct_input {
                error_message += &format!("Need to provide name for {}\n", rn);
            }
        }
        if !correct_input {
            panic!("{}",&error_message)
        }


        // data nodes
        for (key, value) in &self.dnodes {
            // Create the node
            
            let new_id = match leafs.get(&value.id) {
                Some(value) => {value.clone()},
                None => {Node::generate_id()}
            };

            let dnode = DNode {
                id: new_id.clone(),
                template: value.id.clone(),
            };
            // insert into the final
            new_dnodes.insert(new_id.clone(), dnode);
            // insert into the remaping
            dnode_mapping.insert(key, new_id.clone());

        }

        // Create all the new data nodes
        for (key, value) in &self.cnodes {
            let cid = Node::generate_id();

            let map_with_error = |k: &String| {
                dnode_mapping.get(k).cloned().unwrap_or_else(|| {
                    panic!("Failed to find '{}' in input mappings. Aborting.", k);
                })
            };
        
            let cnode = CNode {
                id: cid.clone(),
                template: value.id.clone(),
                incoming: value.incoming.iter().map(map_with_error).collect(),
                outcoming: value.outcoming.iter().map(map_with_error).collect(),
                extra: BTreeMap::new(), // Create empty
            };
        
            new_cnodes.insert(cid.clone(), cnode);
        }



        // Generate a database
        let db = Database {
            template: self.clone(),
            cnodes : new_cnodes,
            dnodes : new_dnodes,
            };
        db
    }

}



impl DatabaseTemplate {

    /// Generates a graph
    /// DiGraph. contains node names
    /// BTreeMap - contains key - graph NodeIndex; value - object id. (allows retrieving actual object)
    /// I use this bocause in some places I want to find the orignal object given the label
    fn generate_digraph(&self) -> (DiGraph::<String, String>, BTreeMap<NodeIndex, String>){
        
        let mut graph = DiGraph::<String, String>::new(); // initialize the final graph
        let mut back_retrieval: BTreeMap<NodeIndex, String> = BTreeMap::new();
        // Define all graph node object and place them into a BTreeMap. Used for constructing the graph
        let mut graph_nodes:  BTreeMap<String, NodeIndex> = BTreeMap::new(); // node storage thing
        let mut edges: Vec<(NodeIndex,NodeIndex)> = Vec::new(); 

        // Create nodes for the graph
        for (id, node) in self.cnodes.iter() {
            let  node_name = node.id.clone();
            let gn = graph.add_node(node_name.clone());
            graph_nodes.insert(id.clone(), gn);
            back_retrieval.insert(gn, id.to_string());
        }
        for (id, node) in self.dnodes.iter() {
            let  node_name = node.id.clone();
            let gn = graph.add_node(node_name.clone());
            graph_nodes.insert(id.clone(), gn);
            back_retrieval.insert(gn, id.to_string());
        }


        // Add edges to the graph
        // Go through all nodes
        for (id, node) in self.cnodes.iter() {
            // Go through all inputs in a node
            for i_id in &node.incoming {
                let starting_node =  match graph_nodes.get(i_id) {
                    Some(value) => value,
                    None => panic!("{}",format!{"Node {} has not been found in the diGraph object.", i_id})
                };
                let end_node = graph_nodes.get(id).expect(&format!("input {} found for {} calculation", &i_id, &id));
                edges.push((*starting_node, *end_node));
            }

            for i_id in &node.outcoming {
                let starting_node = graph_nodes.get(id).expect(&format!("input {} found for {} calculation", &i_id, &id));
                let end_node =  match graph_nodes.get(i_id) {
                    Some(value) => value,
                    None => panic!("{}",format!{"Node {} has not been found in the diGraph object.", i_id})
                };
                edges.push((*starting_node, *end_node));

            }
        }

        graph.extend_with_edges(&edges);
        return (graph, back_retrieval)

    }

    /// Find all root nodes (all input files needed to implement the template.)
    fn find_root_nodes(&self) -> HashSet<IdNodeTemplate> {

        let (graph, mappings) = self.generate_digraph();

        graph
        .node_indices()
        .filter(|&node| graph.neighbors_directed(node, Direction::Incoming).next().is_none())
        .filter_map(|node_id| mappings.get(&node_id).cloned())  // get and clone the IdNodeTemplate
        .collect()
}

}


/// Implement all selection and filtering functions
#[pymethods]
impl Database {


    #[new]
    pub fn new() -> Self {
        let template = DatabaseTemplate {
            cnodes: BTreeMap::new(),
            dnodes: BTreeMap::new(),
        };

        Database {
            template : template,
            cnodes:BTreeMap::new(),
            dnodes:BTreeMap::new(),
        }

    }


    fn __str__(&self) -> PyResult<String> {

        let cnodes = format!("{:?}", self.cnodes);
        let dnodes = format!("{:?}", self.dnodes);
        let template = format!("{:?}", self.template);
        Ok(format!("Database(\ntemplate={}\ncnodes={};\ndnodes={}\n)",template,cnodes, dnodes  ))
    }


    /// Generate an empty database with the same template
    fn generate_empty(&self) -> Database{

        Database{
            dnodes: BTreeMap::new(),
            cnodes: BTreeMap::new(),
            template: self.template.clone()
        }
    }


    /// methods to interact with the template object.
    fn template_register_dnode(&mut self, name:String ) -> DNodeTemplate {
        self.template.register_dnode(name)
    }

    /// Register a new calculation
    /// If a calculation already exists, then update the whole database with the new command.
    /// Add extra information from python that is a dictionary

    fn template_register_cnode(
        &mut self,
        name: String,
        command: String,
        extra: Option<&Bound<'_, PyDict>>,
    ) -> CNodeTemplate {
        // Parse the command and create the node
        let mut new_node = self.template.create_calculation_node(name.clone(), command);

        // If extra is provided from Python, convert it to BTreeMap<String, ExtraData>
        if let Some(py_dict) = extra {
            let mut extra_map = BTreeMap::new();
            for (k, v) in py_dict.iter() {
                let key: String = k.extract().unwrap();
                let value = if let Ok(i) = v.extract::<i32>() {
                    ExtraData::Int(i)
                } else if let Ok(s) = v.extract::<String>() {
                    ExtraData::String(s)
                } else if let Ok(b) = v.extract::<bool>() {
                    ExtraData::Bool(b)
                } else {
                    panic!("Unsupported type for extra data");
                };
                extra_map.insert(key, value);
            }
            new_node.extra = extra_map;
        }

        // Check if the node already exists and is compatible
        match self.template.cnodes.get(&name) {
            Some(old_node) => {
                if new_node != *old_node {
                    panic!("A same node in the template has been found! The new node is different. If you want to overwrite the node use explicit mechanism of search for nodes and manually overwrite.");
                }
                new_node
            }
            None => {
                self.template.cnodes.insert(name.clone(), new_node.clone());
                new_node
            }
        }
    }

    fn template_as_dot(&self) -> String {
        self.template.as_dot()
    }

    pub fn template_create_calculation(&self, leafs: BTreeMap<String, String>) -> Database {
        self.template.create_calculation(leafs)
    }


    pub fn as_dot(&self) -> String {
        let (graph, retrieval) = self.generate_digraph();
        format!("{}", Dot::with_config(&graph, &[Config::EdgeNoLabel]))
    }

    /// Get a DataNode and CalculationNode from a database
    pub fn get(&self, id: String) -> Option<Node>{

        let calculation_branch = self.cnodes.contains_key(&id);
        let data_branch = self.dnodes.contains_key(&id);

        if !calculation_branch && !data_branch {
            return None
        }

        if calculation_branch {
            let node = self.cnodes.get(&id).expect("Failed to find a calculation node.");
            let return_node = Node::Calculation(node.clone());
            return Some(return_node)

        }
        else {
            let node = self.dnodes.get(&id).expect("Failed to find the data node");
            let return_node = Node::Data(node.clone());
            return Some(return_node)
        }
    }

    /// Select all nodes in the same template class
    pub fn select_similar(&self, template_name: String) -> Vec<Node> {
        
        let mut filtered_names: Vec<Node> = Vec::new();

        for (key, cnode) in self.cnodes.iter() {
            if cnode.template == template_name {
                filtered_names.push(Node::Calculation(cnode.clone()));
            }
        }
        for (key, cnode) in self.dnodes.iter() {
            if cnode.template == template_name {
                filtered_names.push(Node::Data(cnode.clone()));
            }
        }

        filtered_names

    }

    /// This is pretty much the same as check agains, but the global database
    /// gets updated too. If there are new template nodes, then the total template
    /// gets expanded
    /// new calculations are also merged
pub fn check_against_and_register(&mut self, global_db: &mut Database){
    // 1. Check template compatibility and expand global template if needed
    for (key, value) in self.template.dnodes.iter() {
        if let Some(global_value) = global_db.template.dnodes.get(key) {
            if value != global_value {
                panic!("Template mismatch for data node '{}': existing and global templates differ", key);
            }
        } else {
            global_db.template.dnodes.insert(key.clone(), value.clone());
        }
    }
    for (key, value) in self.template.cnodes.iter() {
        if let Some(global_value) = global_db.template.cnodes.get(key) {
            if value != global_value {
                panic!("Template mismatch for calculation node '{}': existing and global templates differ", key);
            }
        } else {
            global_db.template.cnodes.insert(key.clone(), value.clone());
        }
    }

    // 2. Merge/rename nodes in self to match global_db where possible
    self.check_against(global_db);

    // 3. Register only new nodes in the global database
    for (key, value) in self.cnodes.iter() {
        if !global_db.cnodes.contains_key(key) {
            global_db.cnodes.insert(key.clone(), value.clone());
        }
    }
    for (key, value) in self.dnodes.iter() {
        if !global_db.dnodes.contains_key(key) {
            global_db.dnodes.insert(key.clone(), value.clone());
        }
    }
}
/// Given the global database - the method adjusts the self database so that if there are calculations that are the same
/// (determined from the leaf nodes and the template name), then
/// the file is renamed.
/// There could be cases where a template node does not exist in the global_db (in this case the database is being expanded with new unseen calculations), in that case just take the calculation by given a warning that such template has not been found.
pub fn check_against(&mut self, global_db: &Database) {
    // 1. Check template compatibility
    for (key, value) in self.template.dnodes.iter() {
        if let Some(global_value) = global_db.template.dnodes.get(key) {
            if value != global_value {
                panic!("Template mismatch for data node '{}': existing and global templates differ", key);
            }
        }
    }
    for (key, value) in self.template.cnodes.iter() {
        if let Some(global_value) = global_db.template.cnodes.get(key) {
            if value != global_value {
                panic!("Template mismatch for calculation node '{}': existing and global templates differ", key);
            }
        } else {
            println!("Warning: Template '{}' not found in global database", key);
        }
    }

    #[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
    enum NodeIdentifier {
        Calculation {
            template: String,
            input_ids: BTreeSet<String>,
        },
        LeafData {
            template: String,
            value: String,
        },
        DerivedData {
            template: String,
            parent_calc: Box<NodeIdentifier>,
        },
    }

    // --- First pass: Merge data nodes (leaf and derived) ---
    let (global_graph, global_retrieval) = global_db.generate_digraph();
    let mut global_identifiers: BTreeMap<NodeIdentifier, String> = BTreeMap::new();

    for node in global_graph.node_indices() {
        let node_id = global_retrieval.get(&node).unwrap();
        let node_obj = global_db.get(node_id.clone()).unwrap();

        let identifier = match &node_obj {
            Node::Calculation(calc) => {
                let mut input_ids = BTreeSet::new();
                for parent in global_graph.neighbors_directed(node, Direction::Incoming) {
                    let parent_id = global_retrieval.get(&parent).unwrap().clone();
                    if let Some(Node::Data(_)) = global_db.get(parent_id.clone()) {
                        input_ids.insert(parent_id);
                    }
                }
                NodeIdentifier::Calculation {
                    template: calc.template.clone(),
                    input_ids,
                }
            }
            Node::Data(data) => {
                // Find parent calculation
                let mut parent_calc = None;
                for parent in global_graph.neighbors_directed(node, Direction::Incoming) {
                    let parent_id = global_retrieval.get(&parent).unwrap();
                    if let Some(Node::Calculation(_)) = global_db.get(parent_id.clone()) {
                        parent_calc = Some(parent_id.clone());
                        break;
                    }
                }
                if let Some(parent_calc_id) = parent_calc {
                    if let Some(Node::Calculation(calc)) = global_db.get(parent_calc_id.clone()) {
                        let parent_node_idx_opt = global_retrieval.iter().find(|(_, v)| *v == &parent_calc_id).map(|(k, _)| *k);
                        if let Some(parent_node_idx) = parent_node_idx_opt {
                            let mut input_ids = BTreeSet::new();
                            for parent in global_graph.neighbors_directed(parent_node_idx, Direction::Incoming) {
                                let parent_id = global_retrieval.get(&parent).unwrap().clone();
                                if let Some(Node::Data(_)) = global_db.get(parent_id.clone()) {
                                    input_ids.insert(parent_id);
                                }
                            }
                            let calc_ident = NodeIdentifier::Calculation {
                                template: calc.template.clone(),
                                input_ids,
                            };
                            NodeIdentifier::DerivedData {
                                template: data.template.clone(),
                                parent_calc: Box::new(calc_ident),
                            }
                        } else {
                            // Could not find parent calculation node index, treat as leaf
                            NodeIdentifier::LeafData {
                                template: data.template.clone(),
                                value: data.id.clone(),
                            }
                        }
                    } else {
                        NodeIdentifier::LeafData {
                            template: data.template.clone(),
                            value: data.id.clone(),
                        }
                    }
                } else {
                    NodeIdentifier::LeafData {
                        template: data.template.clone(),
                        value: data.id.clone(),
                    }
                }
            }
        };

        global_identifiers.insert(identifier, node_id.clone());
    }

    let (self_graph, self_retrieval) = self.generate_digraph();
    let mut rename_map: BTreeMap<String, String> = BTreeMap::new();

    // --- First pass: Data nodes only ---
    for node in self_graph.node_indices() {
        let node_id = self_retrieval.get(&node).unwrap();
        let node_obj = self.get(node_id.clone()).unwrap();

        let identifier = match &node_obj {
            Node::Data(data) => {
                // Find parent calculation
                let mut parent_calc = None;
                for parent in self_graph.neighbors_directed(node, Direction::Incoming) {
                    let parent_id = self_retrieval.get(&parent).unwrap();
                    if let Some(Node::Calculation(_)) = self.get(parent_id.clone()) {
                        parent_calc = Some(parent_id.clone());
                        break;
                    }
                }
                if let Some(parent_calc_id) = parent_calc {
                    if let Some(Node::Calculation(calc)) = self.get(parent_calc_id.clone()) {
                        let parent_node_idx_opt = self_retrieval.iter().find(|(_, v)| *v == &parent_calc_id).map(|(k, _)| *k);
                        if let Some(parent_node_idx) = parent_node_idx_opt {
                            let mut input_ids = BTreeSet::new();
                            for parent in self_graph.neighbors_directed(parent_node_idx, Direction::Incoming) {
                                let parent_id = self_retrieval.get(&parent).unwrap().clone();
                                if let Some(Node::Data(_)) = self.get(parent_id.clone()) {
                                    input_ids.insert(parent_id);
                                }
                            }
                            let calc_ident = NodeIdentifier::Calculation {
                                template: calc.template.clone(),
                                input_ids,
                            };
                            NodeIdentifier::DerivedData {
                                template: data.template.clone(),
                                parent_calc: Box::new(calc_ident),
                            }
                        } else {
                            // Could not find parent calculation node index, treat as leaf
                            NodeIdentifier::LeafData {
                                template: data.template.clone(),
                                value: data.id.clone(),
                            }
                        }
                    } else {
                        NodeIdentifier::LeafData {
                            template: data.template.clone(),
                            value: data.id.clone(),
                        }
                    }
                } else {
                    NodeIdentifier::LeafData {
                        template: data.template.clone(),
                        value: data.id.clone(),
                    }
                }
            }
            _ => continue,
        };

        let is_root = self_graph.neighbors_directed(node, Direction::Incoming).next().is_none();
        if !is_root {
            if let Some(global_node_id) = global_identifiers.get(&identifier) {
                if node_id != global_node_id {
                    rename_map.insert(node_id.clone(), global_node_id.clone());
                }
            }
        }
    }

    self.apply_renames(&rename_map);

    // --- Second pass: Calculation nodes, using possibly renamed data node IDs ---
    let mut renamed_id = |id: &String| -> String {
        rename_map.get(id).cloned().unwrap_or_else(|| id.clone())
    };

    let mut calc_rename_map: BTreeMap<String, String> = BTreeMap::new();

    for node in self_graph.node_indices() {
        let node_id = self_retrieval.get(&node).unwrap();
        let node_obj = self.get(node_id.clone()).unwrap();

        let identifier = match &node_obj {
            Node::Calculation(calc) => {
                let mut input_ids = BTreeSet::new();
                for parent in self_graph.neighbors_directed(node, Direction::Incoming) {
                    let parent_id = self_retrieval.get(&parent).unwrap().clone();
                    if let Some(Node::Data(_)) = self.get(parent_id.clone()) {
                        input_ids.insert(renamed_id(&parent_id));
                    }
                }
                NodeIdentifier::Calculation {
                    template: calc.template.clone(),
                    input_ids,
                }
            }
            _ => continue,
        };

        let is_root = self_graph.neighbors_directed(node, Direction::Incoming).next().is_none();
        if !is_root {
            if let Some(global_node_id) = global_identifiers.get(&identifier) {
                if node_id != global_node_id {
                    calc_rename_map.insert(node_id.clone(), global_node_id.clone());
                }
            }
        }
    }

    self.apply_renames(&calc_rename_map);
}



pub fn merge_into(&mut self, global_db: &mut Database) {
    // 1. Merge templates
    for (key, value) in self.template.dnodes.iter() {
        match global_db.template.dnodes.get(key) {
            Some(global_value) if global_value != value => {
                panic!("Template mismatch for data node '{}'", key);
            }
            None => {
                global_db.template.dnodes.insert(key.clone(), value.clone());
            }
            _ => {}
        }
    }
    for (key, value) in self.template.cnodes.iter() {
        match global_db.template.cnodes.get(key) {
            Some(global_value) if global_value != value => {
                panic!("Template mismatch for calculation node '{}'", key);
            }
            None => {
                global_db.template.cnodes.insert(key.clone(), value.clone());
            }
            _ => {}
        }
    }

    // 2. Merge nodes and build mapping from self IDs to global_db IDs
    let mut id_map: BTreeMap<String, String> = BTreeMap::new();

    fn merge_node(
        node_id: &String,
        self_db: &Database,
        global_db: &mut Database,
        id_map: &mut BTreeMap<String, String>,
    ) -> String {
        if let Some(mapped) = id_map.get(node_id) {
            return mapped.clone();
        }
        let node = self_db.get(node_id.clone()).unwrap();
        match node {
            Node::Data(d) => {
                // Find parent calculation (if any)
                let parent_calc_id = self_db.cnodes.values().find(|c| c.outcoming.contains(&d.id)).map(|c| c.id.clone());
                let global_node_id = if let Some(parent_calc_id) = parent_calc_id {
                    let global_parent_calc_id = merge_node(&parent_calc_id, self_db, global_db, id_map);
                    let found = global_db.dnodes.values().find(|dn| {
                        dn.template == d.template &&
                        global_db.cnodes.values().any(|c| c.outcoming.contains(&dn.id) && c.id == global_parent_calc_id)
                    });
                    if let Some(existing) = found {
                        existing.id.clone()
                    } else {
                        let new_id = d.id.clone();
                        global_db.dnodes.insert(new_id.clone(), d.clone());
                        new_id
                    }
                } else {
                    let found = global_db.dnodes.values().find(|dn| dn.template == d.template && dn.id == d.id);
                    if let Some(existing) = found {
                        existing.id.clone()
                    } else {
                        let new_id = d.id.clone();
                        global_db.dnodes.insert(new_id.clone(), d.clone());
                        new_id
                    }
                };
                id_map.insert(node_id.clone(), global_node_id.clone());
                global_node_id
            }
            Node::Calculation(c) => {
                let mut global_input_ids = Vec::new();
                for input_id in &c.incoming {
                    let global_input_id = merge_node(input_id, self_db, global_db, id_map);
                    global_input_ids.push(global_input_id);
                }
                let found = global_db.cnodes.values().find(|cn| {
                    cn.template == c.template &&
                    cn.incoming == global_input_ids
                });
                let global_calc_id = if let Some(existing) = found {
                    existing.id.clone()
                } else {
                    let mut new_c = c.clone();
                    new_c.incoming = global_input_ids.clone();
                    global_db.cnodes.insert(new_c.id.clone(), new_c.clone());
                    new_c.id.clone()
                };
                id_map.insert(node_id.clone(), global_calc_id.clone());
                global_calc_id
            }
        }
    }

    // Merge all nodes in self into global_db and build id_map
    let all_node_ids: Vec<String> = self.cnodes.keys().chain(self.dnodes.keys()).cloned().collect();
    for node_id in all_node_ids {
        merge_node(&node_id, self, global_db, &mut id_map);
    }

    // 3. Update all references in self to use canonical global_db IDs
    // Update calculation node inputs and outputs
    for c in self.cnodes.values_mut() {
        c.incoming = c.incoming.iter().map(|id| id_map.get(id).cloned().unwrap_or_else(|| id.clone())).collect();
        c.outcoming = c.outcoming.iter().map(|id| id_map.get(id).cloned().unwrap_or_else(|| id.clone())).collect();
    }
    // Update data node IDs if needed (optional, if you want to fully canonicalize)
    let mut new_dnodes = BTreeMap::new();
    for (id, d) in &self.dnodes {
        let new_id = id_map.get(id).cloned().unwrap_or_else(|| id.clone());
        let mut new_d = d.clone();
        new_d.id = new_id.clone();
        new_dnodes.insert(new_id, new_d);
    }
    self.dnodes = new_dnodes;
    // Update calculation node IDs if needed (optional)
    let mut new_cnodes = BTreeMap::new();
    for (id, c) in &self.cnodes {
        let new_id = id_map.get(id).cloned().unwrap_or_else(|| id.clone());
        let mut new_c = c.clone();
        new_c.id = new_id.clone();
        new_cnodes.insert(new_id, new_c);
    }
    self.cnodes = new_cnodes;
}


    pub fn to_snakemake(&self) -> String {
        let mut result = String::new();
        for (id, node) in &self.cnodes {
            let inputs: Vec<String> = node.incoming.iter().map(|i| format!("directory({}/{})","data".to_string(), i)).collect();
            let outputs: Vec<String> = node.outcoming.iter().map(|o| format!("directory({}/{})", "data".to_string(), o)).collect();

            let command_string = self.get_command(id.clone(), "data".to_string());

            let command = format!("rule {}:\n    input: {}\n    output: {}\n    shell: '{}'\n",
                                  id, inputs.join(", "), outputs.join(", "), command_string);
            result.push_str(&command);
        }
        result
    }


    /// Adds a given Database to the existing one.
    /// Merging is minimal - if nodes can be made the same - they will
    /// nodes are same if 1) they have the same template tag; 2) have the same root nodes
    /// The database gets modified in place with addition of new nodes
    /// And the provided database gets returned with some nodes relabeled to match the old database
    pub fn register_pipeline(&mut self, other: Database) -> Database {

        // generate_graphs
        let (this_graph, this_retrieval) = self.generate_digraph();
        let (other_graph, other_retrieval) = other.generate_digraph();

        // Create a new db out of the old ones
        let mut new_cnodes :BTreeMap<IdC, CNode> = BTreeMap::new();
        let mut new_dnodes :BTreeMap<IdD, DNode> = BTreeMap::new();

        // Will store all ids
        let mut mapper: BTreeMap<NodeIdentifier, Node> = BTreeMap::new();
        let mut data_id_overwrites: BTreeMap<String, String> = BTreeMap::new(); // For renaming some of the nodes to keep things in order

        /// Structure that stores data needed to assert if two nodes are the same or not
        #[derive(Debug, PartialEq, Eq)]
        struct NodeIdentifier {
            template: String,
            root_node_names: HashSet<String>,
        }
        impl Ord for NodeIdentifier {
            fn cmp(&self, other: &Self) -> Ordering {
                // First compare templates
                match self.template.cmp(&other.template) {
                    Ordering::Equal => {
                        // Then compare sorted vectors from HashSets
                        let mut self_vec: Vec<_> = self.root_node_names.iter().collect();
                        let mut other_vec: Vec<_> = other.root_node_names.iter().collect();
                        self_vec.sort();
                        other_vec.sort();
                        self_vec.cmp(&other_vec)
                    }
                    ord => ord,
                }
            }
        }
        
        impl PartialOrd for NodeIdentifier {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        fn find_roots_from_node(graph: &DiGraph<String, String>, start: NodeIndex) -> HashSet<String> {
            let mut roots = HashSet::new();
            let mut visited = HashSet::new();
            let mut to_visit = VecDeque::new();

            to_visit.push_back(start);

            while let Some(node) = to_visit.pop_front() {
                // If already visited, skip
                if !visited.insert(node) {
                    continue;
                }

                let parents: Vec<_> = graph.neighbors_directed(node, Direction::Incoming).collect();

                if parents.is_empty() {
                    // No parents => root node
                    roots.insert(graph[node].clone());
                } else {
                    // Continue exploring parents
                    for parent in parents {
                        to_visit.push_back(parent);
                    }
                }
            }
            roots
        }



        // Go through the other object
        for node in other_graph.node_indices() {
            let roots = find_roots_from_node(&other_graph,node);
            let node_id = other_retrieval.get(&node).unwrap();
            let node_obj = other.get(node_id.clone()).unwrap();

            let (template, id) = match node_obj {
                Node::Calculation(value) => {
                    let template = value.template;
                    let x = other.get(value.id.clone()).expect(&format!("could not find {} in the other database",value.id.clone()));
                    (template, x)
                }
                Node::Data(value) => {
                    let template = value.template;
                    let x = other.get(value.id.clone()).expect(&format!("could not find {} in the other database",value.id.clone()));
                    (template, x)
                }
            };
            mapper.insert(NodeIdentifier {template :template, root_node_names : roots},id);
        }

        // Go through the self object
        for node in this_graph.node_indices() {
            let roots = find_roots_from_node(&this_graph,node);
            let node_id = this_retrieval.get(&node).unwrap();
            let node_obj = self.get(node_id.clone()).unwrap();

            let (template, id) = match &node_obj {
                Node::Calculation(value) => {
                    let template = value.template.clone();
                    let x = self.get(value.id.clone()).expect(&format!("could not find {} in the this database",value.id.clone()));
                    (template, x)
                }
                Node::Data(value) => {
                    let template = value.template.clone();
                    let x = self.get(value.id.clone()).expect(&format!("could not find {} in the this database",value.id.clone()));
                    (template, x)
                    
                }
            };
            
            
            let node_identifier = NodeIdentifier {template :template.clone(), root_node_names : roots.clone()};

            // Check if this key already exists. If id does, then if it's a computing node
            // the key needs to be modified.

            if let Some(mapped) = mapper.get(&node_identifier) {
                // Insert the id I found and insert the new id
                match &node_obj {
                    Node::Calculation(value) => {
                        data_id_overwrites.insert(mapped.id(), value.id.clone());
                    },
                    Node::Data(value) => {
                        data_id_overwrites.insert(mapped.id(), value.id.clone());
                    },
                }
            }

            mapper.insert(NodeIdentifier {template :template, root_node_names : roots},id);
        }

        // Create a new database

        for (key, value) in mapper.iter(){
            match value {
                Node::Calculation(value) => {
                    let mut insert_cnode = value.clone();
                    
                    for v in &mut insert_cnode.incoming {
                        if let Some(replacement) = data_id_overwrites.get(v) {
                            *v = replacement.clone();
                        }
                    }

                    for v in &mut insert_cnode.outcoming {
                        if let Some(replacement) = data_id_overwrites.get(v) {
                            *v = replacement.clone();
                        }
                    }

                    new_cnodes.insert(value.id.clone(),insert_cnode);
                },
                Node::Data(value) => {new_dnodes.insert(value.id.clone(), value.clone());}
            }



        }

        // Merge templates
        // Go through all the nodes. If the nodes are the same, then overwrite, if the nodes are different, then crash, otherwise merge the two templates in the same fashion as before

        let mut new_template = other.template.clone();

        // Commented out because I do not want to change the other template. 
        // When returning an object it should not change the template (as in calculation I am trying to achieve
        // a particuaal goal)
        // Only the self template needs to expand as I might register  new things
        // The global template is for keeping track of all the calculations.
        for (key, value) in self.template.dnodes.iter() {
            match new_template.dnodes.get(key){
                Some(v) => {
                    if v != value {
                        panic!("Two data nodes are given, but they are different. Templates need to be compatable to merge.");
                    }; 
                }
                None => {new_template.dnodes.insert(key.clone(), value.clone());}
            }
        }


        for (key, value) in self.template.cnodes.iter() {
            match new_template.cnodes.get(key){
                Some(v) => {
                    if v != value {
                        panic!("Two data nodes are given, but they are different. Templates need to be compatable to merge.{:?}; {:?}", v, value);
                    }; 
                }
                None => {
                    for dkey in value.incoming.iter() {
                        if !new_template.dnodes.contains_key(dkey) {
                            panic!("Data needed for a calculation not found");
                        }
                    }
                    for dkey in value.outcoming.iter() {
                        if !new_template.dnodes.contains_key(dkey) {
                            panic!("Data needed for a calculation not found");
                        }
                    }

                    new_template.cnodes.insert(key.clone(), value.clone());
                }
            }
            };


        // Change self to the merged nodes
        self.cnodes = new_cnodes.clone();
        self.dnodes = new_dnodes.clone();
        self.template = new_template.clone();
        

        // Create and return the modified database
        // Just go through the database and if a nodes is found among the other database, add the new one
        
        let mut mutted_other = Database{
            dnodes: BTreeMap::new(),
            cnodes: BTreeMap::new(),
            template: other.template.clone()
        };


        for (key, value) in other.dnodes.iter() {
            match data_id_overwrites.get(key){
                Some(new_key) =>{
                    // Data node has been overwritten and I need to get the new one
                    let new_value = new_dnodes.get(new_key).expect("failed to find a key");
                    mutted_other.dnodes.insert(new_key.clone(), new_value.clone());
                }
                None =>{
                    // The node has not been overwritten. 
                    let new_value = new_dnodes.get(key).expect("failed to find a key");
                    mutted_other.dnodes.insert(key.clone(), new_value.clone());
                }
            }
        }

        for (key, value) in other.cnodes.iter() {
            match data_id_overwrites.get(key){
                Some(new_key) =>{
                    // Data node has been overwritten and I need to get the new one
                    let new_value = new_cnodes.get(new_key).expect("failed to find a key");
                    mutted_other.cnodes.insert(new_key.clone(), new_value.clone());
                }
                None =>{
                    // The node has not been overwritten. 
                    let new_value = new_cnodes.get(key).expect("failed to find a key");
                    mutted_other.cnodes.insert(key.clone(), new_value.clone());
                }
            }
        }

        
        // Go through all the nodes in the other database
        // If data_id_overwirte contains the key, then overwrite that node with the renamed one
        // Otherwise overwrite with the new_node (because some values could have changed.)

        // In case 
        // for (key1, key2) in data_id_overwrites.iter() {
        //     // Insert modified node
        //     if other.dnodes.contains_key(key2) {
        //         // Delete the old
        //         changed_other.dnodes.remove(key2);
        //         //Insert new
        //         changed_other.dnodes.insert(key1.clone(), new_dnodes.get(key1).expect("should not fail").clone());
        //     }


        // }
        // for (key1, key2) in data_id_overwrites.iter() {
        //     if other.cnodes.contains_key(key2) {
        //         // Delete the old
        //         changed_other.cnodes.remove(key2);
        //         //Insert new
        //         changed_other.cnodes.insert(key1.clone(), new_cnodes.get(key1).expect("should not fail").clone());
        //     }

        // }


        mutted_other

    }


    /// Create a single new data node
    /// Used when the database is being changed manually.
    pub fn register_dnode(&mut self, template_id: String, name:Option<String>) -> DNode {

        // check if the template id exists amond the template
        if !self.template.dnodes.contains_key(&template_id) {
            panic!("DNode with template id {} does not exist. Make sure that this type of dnode is registered among the templates and that your're providing a Data Node.", template_id);
        }

        let node_id = match name {
            Some(value) => value,
            None => Node::generate_id()
            };

        // Create a dnode
        let new_dnode = DNode { id: node_id.clone(),
                                template: template_id.clone(),
                            };

        self.dnodes.insert(node_id, new_dnode.clone());

        new_dnode

    }

    /// In case calculation changes this command allows fixind the database.
    /// Provide the calculation name and the new command.
    /// The final argument determines how to correct the database. Which nodes to correct
    /// There could be 1) New nodes, 2) Local connection of nodes (therefore, need to porvide how to match all nodes)
    /// calculation_name - template name of the calculation
    /// new_command - string that specifies the new command to overwrite with
    /// database_corrections BTreeMap<'calculation id', 'BTreeMap<'data node template id', (["name","filename of the new dnode"]|"id", "id of an existing dnode"])>>
    pub fn overwrite_calculation(&mut self, calculation_name: String, new_command:String, database_correction:BTreeMap<String, BTreeMap<String, String>>) {
        // Check if all calculation nodes are provided.
        // Also make sure that all inputs and outputs are specfied. Fully overwrite every node.
        // This dictionary can be generated by using the selection functions and the database.

        // Check the inputs

    }

    /// Selects Future of given Node
    pub fn select_future(&self, name: String) -> Database {
        let subgraph = self.select_node_future(name);
        let graph = self.digraph_to_database(&subgraph);
        graph
    }

    /// Select History of a given node
    pub fn select_history(&self, name: String) -> Database {
        let subgraph = self.select_node_history(name);
        let graph = self.digraph_to_database(&subgraph);
        graph
    }

    // /// Select template history
    // /// Used for subselecting templates. Allows creating partial workflows.
    // /// Need to filder the database and the calculations, so that the data in the template match.
    // pub fn select_template_hisotyr(&self, name:String) -> Database {

    // }

    /// Convert to nodes
    pub fn to_nodes(&self) -> Vec<Node> {
        unimplemented!();
    }

    /// generate the full command to run.
    /// root_folder - prepend a string to all commands.
    fn get_command(&self,cnode_id: String, root_folder: String) -> String {

        // Go through all inputs and outputs and replace them with appropriate inputs

        let cnode = self.cnodes.get(&cnode_id).expect(&format!("Failed to find '{}' among the calculation nodes!", cnode_id));

        let template_cnode = self.template.cnodes.get(&cnode.template).expect("Failed to find the template node.");

        let mut full_command = template_cnode.command.clone();


        for (i, i_id) in cnode.incoming.iter().enumerate() {
            full_command = full_command.replace(&format!("$i_{}", i), &format!("{}/{}",root_folder,i_id));
        }


        for (o, o_id) in cnode.outcoming.iter().enumerate() {
            full_command = full_command.replace(&format!("$o_{}", o), &format!("{}/{}",root_folder,o_id));
        }

        // Replace all instances with extra(key) with key, where key is extracted from the node or template. If value is not found, then panic
        let mut replaced_keys = std::collections::HashSet::new();
        for (key, _) in &template_cnode.extra {
            let value = if let Some(val) = cnode.extra.get(key) {
                val
            } else if let Some(val) = template_cnode.extra.get(key) {
                val
            } else {
                panic!("Extra value for key '{}' not found in node or template!", key);
            };
            let value_str = match value {
                ExtraData::Int(i) => i.to_string(),
                ExtraData::String(s) => s.clone(),
                ExtraData::Bool(b) => b.to_string(),
            };
            full_command = full_command.replace(&format!("extra({})", key), &value_str);
            replaced_keys.insert(key.clone());
        }
        // Also handle any extra keys that are only in the instance node (not in the template)
        for (key, value) in &cnode.extra {
            if replaced_keys.contains(key) {
                continue;
            }
            let value_str = match value {
                ExtraData::Int(i) => i.to_string(),
                ExtraData::String(s) => s.clone(),
                ExtraData::Bool(b) => b.to_string(),
            };
            full_command = full_command.replace(&format!("extra({})", key), &value_str);
        }


        full_command

    }

    pub fn write(&self, folder: String) -> PyResult<()> {
        let path = Path::new(&folder);

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                pyo3::exceptions::PyIOError::new_err(format!("Failed to create directory: {e}"))
            })?;
        }

        let mut file = std::fs::File::create(&path).map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("Failed to create file: {e}"))
        })?;

        let write_string = serde_json::to_string_pretty(self).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Serialization failed: {e}"))
        })?;

        file.write_all(write_string.as_bytes()).map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("Failed to write file: {e}"))
        })?;

        Ok(())
    }



    /// Read the databsase.
    /// If a file exist then it's read as normal.
    /// If it does not exist, then a new database is returned.
    #[classmethod]
    pub fn read(_cls: &Bound<'_, PyType>, path: String) -> PyResult<Self> {

        let path_o = Path::new(&path);

        if path_o.exists(){
            let content = std::fs::read_to_string(path_o).map_err(|e| {
                pyo3::exceptions::PyIOError::new_err(format!("Failed to read file: {e}"))
            })?;

            serde_json::from_str(&content).map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!("Failed to parse JSON: {e}"))
            })
        }
        else {
            println!("File not found. Initializing an empty database.");
            let db = Database {
                template: DatabaseTemplate {
                    cnodes : BTreeMap::new(),
                    dnodes : BTreeMap::new()
                },
                cnodes: BTreeMap::new(),
                dnodes: BTreeMap::new()
            };
            Ok(db)
        }

    }



}

impl Database{

    /// Generates a graph
    /// DiGraph. contains node names
    /// BTreeMap - contains key - graph NodeIndex; value - object id. (allows retrieving actual object)
    /// I use this bocause in some places I want to find the orignal object given the label
    fn generate_digraph(&self) -> (DiGraph::<String, String>, BTreeMap<NodeIndex, String>){
        
        let mut graph = DiGraph::<String, String>::new(); // initialize the final graph
        let mut back_retrieval: BTreeMap<NodeIndex, String> = BTreeMap::new();
        // Define all graph node object and place them into a BTreeMap. Used for constructing the graph
        let mut graph_nodes:  BTreeMap<String, NodeIndex> = BTreeMap::new(); // node storage thing
        let mut edges: Vec<(NodeIndex,NodeIndex)> = Vec::new(); 

        // Create nodes for the graph
        for (id, node) in self.cnodes.iter() {

            let gn = graph.add_node(node.id.clone());
            graph_nodes.insert(id.clone(), gn);
            back_retrieval.insert(gn, id.to_string());
        }
        for (id, node) in self.dnodes.iter() {

            let gn = graph.add_node(node.id.clone());
            graph_nodes.insert(id.clone(), gn);
            back_retrieval.insert(gn, id.to_string());
        }


        // Add edges to the graph
        // Go through all nodes
        for (id, node) in self.cnodes.iter() {
            // Go through all inputs in a node
            for i_id in &node.incoming {
                let starting_node =  match graph_nodes.get(i_id) {
                    Some(value) => value,
                    None => panic!("{}",format!{"Node {} has not been found in the diGraph object.", i_id})
                };
                let end_node = graph_nodes.get(id).expect(&format!("input {} found for {} calculation", &i_id, &id));
                edges.push((*starting_node, *end_node));
            }

            for i_id in &node.outcoming {
                let starting_node = graph_nodes.get(id).expect(&format!("input {} found for {} calculation", &i_id, &id));
                let end_node =  match graph_nodes.get(i_id) {
                    Some(value) => value,
                    None => panic!("{}",format!{"Node {} has not been found in the diGraph object.", i_id})
                };
                edges.push((*starting_node, *end_node));

            }
        }

        graph.extend_with_edges(&edges);
        return (graph, back_retrieval)

    }

    /// Helper method to apply renames to the database
    fn apply_renames(&mut self, rename_map: &BTreeMap<String, String>) {
        // Create new maps for renamed nodes
        let mut new_cnodes = BTreeMap::new();
        let mut new_dnodes = BTreeMap::new();

        // Rename calculation nodes
        for (old_id, cnode) in self.cnodes.iter() {
            let new_id = rename_map.get(old_id).unwrap_or(old_id);
            
            let mut new_cnode = cnode.clone();
            new_cnode.id = new_id.clone();
            
            // Update incoming and outgoing references
            for incoming_ref in &mut new_cnode.incoming {
                if let Some(new_ref) = rename_map.get(incoming_ref) {
                    *incoming_ref = new_ref.clone();
                }
            }
            for outgoing_ref in &mut new_cnode.outcoming {
                if let Some(new_ref) = rename_map.get(outgoing_ref) {
                    *outgoing_ref = new_ref.clone();
                }
            }
            
            new_cnodes.insert(new_id.clone(), new_cnode);
        }

        // Rename data nodes
        for (old_id, dnode) in self.dnodes.iter() {
            let new_id = rename_map.get(old_id).unwrap_or(old_id);
            
            let mut new_dnode = dnode.clone();
            new_dnode.id = new_id.clone();
            
            new_dnodes.insert(new_id.clone(), new_dnode);
        }

        // Replace the old maps with the new ones
        self.cnodes = new_cnodes;
        self.dnodes = new_dnodes;
    }
    pub fn digraph_to_database(&self, graph: &DiGraph<String, ()>) -> Database {

        let mut cnodes: BTreeMap<String, CNode> = BTreeMap::new();
        let mut dnodes: BTreeMap<String, DNode> = BTreeMap::new();

        for node_id in graph.node_indices() {
            let node_name = graph.node_weight(node_id).expect("Failed to get a node name");
            
            // handle the calculation node 
            if self.cnodes.contains_key(node_name) {
                cnodes.insert(node_name.clone(), self.cnodes.get(node_name).expect("failed").clone());
                continue
            }   
            if self.dnodes.contains_key(node_name) {
                dnodes.insert(node_name.clone(), self.dnodes.get(node_name).expect("failed").clone());
                continue
            }
        }

        Database {cnodes : cnodes, dnodes : dnodes, template: self.template.clone()}
    }


    pub fn select_node_history(&self, name: String) -> DiGraph<String, ()> {

        let mut new_graph: DiGraph<String, ()> = DiGraph::new();
        let (current_graph, current_node_name_map) = self.generate_digraph();
        let origin_node = current_graph.node_indices().find(|&node| current_graph[node] == name).expect("failed to find the origin node! Wrong name provided.");
        
        // Create a mapping between original node indices and new node indices
        let mut node_mapping: BTreeMap<NodeIndex, NodeIndex> = BTreeMap::new();
        
        // Find all calculation nodes that needed to produce the calculation.
        for (node_index, node_name) in current_node_name_map.iter() {
            if self.cnodes.contains_key(node_name) && has_path_connecting(&current_graph, *node_index, origin_node, None) {
                // Create a new node with an owned String
                let new_idx = new_graph.add_node(node_name.clone());
                node_mapping.insert(*node_index, new_idx);

                // Instert calculation nodes inputs and outputs to the mapping
                for input_name in &self.cnodes.get(node_name).expect("failed to find a calculation node").incoming {
                    let new_idx = new_graph.add_node(input_name.clone());
                    let old_idx = current_graph.node_indices().find(|&node| current_graph[node] == name).expect("Failed to find a node");
                    node_mapping.insert(old_idx, new_idx);

                }
                for output_name in &self.cnodes.get(node_name).expect("failed to find a calculation node").outcoming {
                    let new_idx = new_graph.add_node(output_name.clone());
                    let old_idx = current_graph.node_indices().find(|&node| current_graph[node] == name).expect("Failed to find a node");
                    node_mapping.insert(old_idx, new_idx);
                }
            }
        }



        
        // Now add the edges between the nodes in the new graph
        for (node_index,node_name) in current_node_name_map.iter() {
            if let Some(&new_idx) = node_mapping.get(node_index) {
                if self.cnodes.contains_key(node_name) {
                    let calc_node = self.cnodes.get(node_name).expect("failed to get the node.");
                    
                    // Add edges for inputs
                    for inp in &calc_node.incoming {
                        if let Some(input_node) = current_graph.node_indices().find(|&node| current_graph[node] == inp.clone()) {
                            if let Some(&new_input_idx) = node_mapping.get(&input_node) {
                                new_graph.add_edge(new_input_idx, new_idx, ());
                            }
                        }
                    }
                    
                    // Add edges for outputs
                    for outp in &calc_node.outcoming {
                        if let Some(output_node) = current_graph.node_indices().find(|&node| current_graph[node] == outp.clone()) {
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



    pub fn select_node_future(&self, name: String) -> DiGraph<String, ()> {

        let mut new_graph: DiGraph<String, ()> = DiGraph::new();
        let (mut current_graph, current_node_name_map) = self.generate_digraph();
        let origin_node = current_graph.node_indices().find(|&node| current_graph[node] == name).expect("failed to find the origin node! Wrong name provided.");
        current_graph.reverse();
        // Create a mapping between original node indices and new node indices
        let mut node_mapping: BTreeMap<NodeIndex, NodeIndex> = BTreeMap::new();
        
        // Find all calculation nodes that needed to produce the calculation.
        for (node_index, node_name) in current_node_name_map.iter() {
            if self.cnodes.contains_key(node_name) && has_path_connecting(&current_graph, *node_index, origin_node, None) {
                // Create a new node with an owned String
                let new_idx = new_graph.add_node(node_name.clone());
                node_mapping.insert(*node_index, new_idx);

                // Instert calculation nodes inputs and outputs to the mapping
                for input_name in &self.cnodes.get(node_name).expect("failed to find a calculation node").incoming {
                    let new_idx = new_graph.add_node(input_name.clone());
                    let old_idx = current_graph.node_indices().find(|&node| current_graph[node] == name).expect("Failed to find a node");
                    node_mapping.insert(old_idx, new_idx);

                }
                for output_name in &self.cnodes.get(node_name).expect("failed to find a calculation node").outcoming {
                    let new_idx = new_graph.add_node(output_name.clone());
                    let old_idx = current_graph.node_indices().find(|&node| current_graph[node] == name).expect("Failed to find a node");
                    node_mapping.insert(old_idx, new_idx);
                }
            }
        }



        
        // Now add the edges between the nodes in the new graph
        for (node_index,node_name) in current_node_name_map.iter() {
            if let Some(&new_idx) = node_mapping.get(node_index) {
                if self.cnodes.contains_key(node_name) {
                    let calc_node = self.cnodes.get(node_name).expect("failed to get the node.");
                    
                    // Add edges for inputs
                    for inp in &calc_node.incoming {
                        if let Some(input_node) = current_graph.node_indices().find(|&node| current_graph[node] == inp.clone()) {
                            if let Some(&new_input_idx) = node_mapping.get(&input_node) {
                                new_graph.add_edge(new_input_idx, new_idx, ());
                            }
                        }
                    }
                    
                    // Add edges for outputs
                    for outp in &calc_node.outcoming {
                        if let Some(output_node) = current_graph.node_indices().find(|&node| current_graph[node] == outp.clone()) {
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

    /// Get extra information about a comutational node
    /// Check if it exists under the node itself
    /// if not then check if it exists in the template
    fn get_extra(&self, node_id: &str, key: &str) -> Option<String> {
        // Check if the node exists in the cnodes
        if let Some(cnode) = self.cnodes.get(node_id) {
            if let Some(value) = cnode.extra.get(key) {
                return Some(match value {
                    ExtraData::Int(i) => i.to_string(),
                    ExtraData::String(s) => s.clone(),
                    ExtraData::Bool(b) => b.to_string(),
                });
            }
        }
        
        // If not found in node, check in the template
        if let Some(template_cnode) = self.template.cnodes.get(node_id) {
            if let Some(value) = template_cnode.extra.get(key) {
                return Some(match value {
                    ExtraData::Int(i) => i.to_string(),
                    ExtraData::String(s) => s.clone(),
                    ExtraData::Bool(b) => b.to_string(),
                });
            }
        }
        
        // If not found anywhere, return None
        None
    }



}




#[pymodule]
fn graphrlib(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Database>()?;
    m.add_class::<DatabaseTemplate>()?;
    Ok(())
}
