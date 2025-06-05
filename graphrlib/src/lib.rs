
/*
DNode - Only contains abstract calculations. real data are only described by location 

*/
use std::collections::BTreeMap;
use pyo3::prelude::*;
use serde::{Serialize, Deserialize};
use regex::Regex;
use std::time::{SystemTime, UNIX_EPOCH};
use std::fmt;

use petgraph::graph::{NodeIndex, DiGraph, UnGraph};
use petgraph::dot::{Dot, Config};
use petgraph::algo::has_path_connecting;



/// Set types for interacting with the database
type IdCTemplate = String;
type IdDTemplate = String;
type IdC = String;
type IdD = String;
type IdTemplate = String;




/// Describe Abstract Data and Calculation nodes
#[pyclass]
#[derive(Serialize, Deserialize, Default,Clone, Debug)]
pub struct DNodeTemplate {
    id: IdDTemplate
}


#[pyclass]
#[derive(Serialize, Deserialize, Default,Clone, Debug)]
pub struct CNodeTemplate {
    pub id: IdCTemplate,
    pub command: String,
    pub incoming: Vec<IdDTemplate>,
    pub outcoming: Vec<IdDTemplate>
}

#[pyclass]
#[derive(Clone)]
enum NodeTemplate {
    Calculation(CNodeTemplate),
    Data(DNodeTemplate),
}


/// Describes abstract calculations.
#[pyclass]
#[derive(Clone)]
pub struct DatabaseTemplate {
    id: IdTemplate,
    cnodes: BTreeMap<IdCTemplate, CNodeTemplate>, // Store all calculation nodes
    dnodes: BTreeMap<IdDTemplate, DNodeTemplate>, // Store all data nodes
}

/// Describes implementations and actual calculations
#[pyclass]
pub struct Database {

    template: DatabaseTemplate, // Store the template
    cnodes: BTreeMap<IdC, CNode>, // Store all calculation nodes
    dnodes: BTreeMap<IdD, DNode>, // Store all data nodes

}


/// describe imlementations of nodes (These will have names with time stamps)
#[pyclass]
#[derive(Serialize, Deserialize, Default,Clone, Debug)]
pub struct DNode {
    pub id: IdD,
    pub template: IdDTemplate
}

/// Describes an abstract calculation node
#[pyclass]
#[derive(Serialize, Deserialize, Default,Clone, Debug)]
pub struct CNode{
    pub id: IdC,
    pub template: IdCTemplate,
    pub command: String,
    pub incoming: Vec<IdD>,
    pub outcoming: Vec<IdD>
}

#[pyclass]
#[derive(Clone)]
enum Node {
    Calculation(CNode),
    Data(DNode),
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

}



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
        Ok(format!("DNodeTemplate(id={};\ncommand={},\ninput={:?},\noutput={:?})", self.id, self.command,self.incoming,self.outcoming ))
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


impl CNode {
    pub fn create_label(&self) -> String{
        let uuid = self.id.clone();
        let base_name = self.template.clone();
        format!("{}{}",uuid, base_name)
    }
}

impl DNode {
    pub fn create_label(&self) -> String{
        let uuid = self.id.clone();
        let base_name = self.template.clone();
        format!("{}{}",uuid, base_name)
    }
}



#[pymethods]
impl DatabaseTemplate {

    #[new]
    pub fn new(name: IdCTemplate) -> Self {
        DatabaseTemplate {
            id : name,
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

    /// Register calculation node
    /// If a key already exists, then comprate the value. If the values are the same do nothing,
    /// If they are different then crash, otherwise, overwrite flag is passed
    pub fn register_cnode(&mut self, name: String, command: String) -> CNodeTemplate {
        


        let values = CNodeTemplate::parse_command(command);

        let cnode = CNodeTemplate {
            id: name.clone(),
            incoming: values.1,
            outcoming: values.2,
            command: values.0,
        };

        self.cnodes.insert(name, cnode.clone());
        cnode

    }

    /// Return the database in DOT format
    pub fn as_dot(&self) -> String {
        let graph = self.generate_digraph();
        format!("{}", Dot::with_config(&graph, &[Config::EdgeNoLabel]))
    }


    /// get a node from a template
    pub fn get(&self, name: String) -> NodeTemplate {
        unimplemented!();
    }

    /// Create an implementation of a given template.
    pub fn create_calculation(&self, leafs: Vec<(String,String)>) -> Database {
        
        // Need to go through all data and calculation nodes and generate actual calculations.

        // Go through all Data nodes

        let mut new_dnodes  = BTreeMap::new();
        let mut new_cnodes = BTreeMap::new();
        let mut dnode_mapping = BTreeMap::new(); // for fully mapping data nodes
        
        // data nodes
        for (key, value) in &self.dnodes {
            // Create the node
            let cid = Node::generate_id();

            let dnode = DNode {
                id: cid.clone(),
                template: value.id.clone(),
            };
            // insert into the final
            new_dnodes.insert(cid.clone(), dnode);
            // insert into the remaping
            dnode_mapping.insert(key, cid.clone());

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
                command: value.command.clone(),
                incoming: value.incoming.iter().map(map_with_error).collect(),
                outcoming: value.outcoming.iter().map(map_with_error).collect(),
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

    fn generate_digraph(&self) -> DiGraph::<String, String>{
        
        let mut graph = DiGraph::<String, String>::new(); // initialize the final graph
        // Define all graph node object and place them into a BTreeMap. Used for constructing the graph
        let mut graph_nodes:  BTreeMap<String, NodeIndex> = BTreeMap::new(); // node storage thing
        let mut edges: Vec<(NodeIndex,NodeIndex)> = Vec::new(); 

        // Create nodes for the graph
        for id in self.cnodes.keys() {
            let gn = graph.add_node(id.clone());
            graph_nodes.insert(id.clone(), gn);
        }
        for id in self.dnodes.keys() {
            let gn = graph.add_node(id.clone());
            graph_nodes.insert(id.clone(), gn);
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
        return graph

    }
}


/// Implement all selection and filtering functions
#[pymethods]
impl Database {

    fn __str__(&self) -> PyResult<String> {

        let cnodes = format!("{:?}", self.cnodes);
        let dnodes = format!("{:?}", self.dnodes);
        Ok(format!("DatabaseTemplate(cnodes={};\ndnodes={})",cnodes, dnodes  ))
    }

    pub fn as_dot(&self) -> String {
        let graph = self.generate_digraph();
        format!("{}", Dot::with_config(&graph, &[Config::EdgeNoLabel]))
    }

    /// Select all nodes based on name
    pub fn select(&self, name: Node) -> Database {
        unimplemented!();
    }

    /// Selects Future of given Node
    pub fn select_future(&self, start: DNode) -> Database {
        unimplemented!();
    }

    /// Select History of a given node
    pub fn select_history(&self, start:DNode) -> Database {
        unimplemented!();
    }

    /// Convert to nodes
    pub fn to_nodes(&self) -> Vec<Node> {
        unimplemented!();
    }

    fn read(&self) -> Database {
        unimplemented!();
    }

    fn write(&self) -> Database {
        unimplemented!();
    }

}

impl Database{


    fn generate_digraph(&self) -> DiGraph::<String, String>{
        
        let mut graph = DiGraph::<String, String>::new(); // initialize the final graph
        // Define all graph node object and place them into a BTreeMap. Used for constructing the graph
        let mut graph_nodes:  BTreeMap<String, NodeIndex> = BTreeMap::new(); // node storage thing
        let mut edges: Vec<(NodeIndex,NodeIndex)> = Vec::new(); 

        // Create nodes for the graph
        for (id, node) in self.cnodes.iter() {
            let gn = graph.add_node(node.create_label());
            graph_nodes.insert(id.clone(), gn);
        }
        for (id, node) in self.dnodes.iter() {
            let gn = graph.add_node(node.create_label());
            graph_nodes.insert(id.clone(), gn);
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
        return graph

    }

}



#[pymodule]
fn graphrlib_test(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Database>()?;
    m.add_class::<DatabaseTemplate>()?;
    Ok(())
}
