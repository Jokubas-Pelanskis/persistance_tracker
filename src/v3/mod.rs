/*
In this approach there is the database is made very small and most of the things are computed when needed.
The main inovation is  that I deterministically calculate names for files based on the template and root nodes. (Assume that the pipeline is fully deterministic)
Using this approach I only need to  save the template and the root nodes.
If I want I can add caching of results.
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


/// defines a single node
struct Node {

}

/// defines the main structure that stores all the information
pub struct Database {

}


impl Database {
    
    /// Connect to the database
    pub fn connect(&mut self, database_path: &str) {}

    /// Convert database to dot format
    pub fn to_dot(& self){}

    /// Given the `global_db` this database will be compared agains and merged where possible
    /// the global db is not modified
    pub fn merge_into(&mut self, global_db: &Database){}

    /// same as merge_into, but in this case missing calculations and data nodes are added to the global database.
    pub fn merge_into_and_commit(&mut self, global_db: &Database){}

    /// for a ginen node id selects all nodes that follow it
    pub fn select_future(&self) -> Database {}

    /// for a given node id selects all nodes that supreseed it 
    pub fn select_history(&self) -> Database {}

    /// For a given node it selecets all nodes that come in and go out
    pub fn select_branch(&self) -> Database {}

    /// Returns only a fraction of the database (template and nodes)
    /// that correspond to the history of the given template node id
    pub fn template_select_history(&self) -> Database {}

    /// Create a new calculation for a given node
    /// template corresponding to self node are created. (the calculation is small)
    pub fn create_calculation(&self) -> Database {}

    /// register template node
    pub fn template_register_dnode(&mut self){}

    /// register template node
    pub fn template_register_cnode(&mut self) {}


}



impl Database{

    /// Calculates the node name.
    /// Algorithm: calculate a hash based on all the relevant information that uniquely determines the calculation or data node.
    /// relevant information: 1) all root node names (node name are unique); 2) calculation names*
    /// * there could be cases where a->c1->b and a->c2->b. Different calculation but produce or connect same type of data.
    /// For example, such cases occur when I slighly modify the calculation (say change the optimisation algorithm or other feature that does not change the output data format or logic) 
    fn calculate_node_name(&self, description: ???) -> {}




    

}






