/*
Optios for the implementation
1) precalculate all subgraphs and store them in the database. Then the query will be really fast.
2) store as linked list and then recursively search. (searching could be slow)
2.1) In each iteration go to disk and read the information you want. This could be very slow in tranversing the list.
2.2) load the whole database into memory and then do the search. Could become problematic if the database is very large.
What will I be searching for
1) all the previous nodes - to determine the name of the node and in searching data analyis step (what was the input for this figure) (I want this to be very fast.)
2) all nodes that fall under the same template name.
What are other relevant constraits to make the decision?
1) I do not expect really deep graphs (transversal list would not be that deep) and they will not be deeply connected. This 
This favours option1.

In this variant implement a sql database where each connection is already precomputed.

SQL database schema:

table template_calculation_nodes
    id INTEGER
    name TEXT - name of the template calculation node

table template_data_nodes
    id INTEGER
    name TEXT - name of the template calculation node
    command TEXT - command name (general expression that has to be parsed to generate the actual command name)

table template_edges (NOTE: this will only store inptus and outputs, not the full graph)
    calculation_id: INTEGER - name of the template calculation node
    parameter_name: TEXT - name of the input/output
    parameter_value: INTEGER - id of a template_data_node

table calcualtion_nodes
    id INTEGER
    name TEXT - name of the node. Hash under which the calculation will be stored
    template_name TEXT
    command TEXT - full command that should be run.

table data_nodes
    id INTEGER
    name TEXT

table edges (NOTE: essentially this stores a dense graph of all inputs and ouputs.)
    source_id INTEGER- name of the node I am inspecting
    target_id INTEGER - name of the node that the anchor has as history 

table extra_calculation_parameters
    id INTEGER
    calculation_id INTEGER - id of the calculation
    extra_name TEXT - name of the extra parameter
    extra_value TEXT - value of the extra parameter (could be int or float or anything.)

Question: do I really need to write my database? Everytime I reconstruct the database form the input files (do that to check for changes and if there are
new calculations). Saving it before was improtant because file namimng was not determenistic; now with this scheme, where node name is determined from root
and all previous calculations - is deterministing; Therefore, for such purpose I do not need to save anything. However. I might want to load the database in 
some other contexts (such as in a script when looking for a neighbour or when running an optimisation algorithm.) Also, I might want to save a bunch of databases
from multiple places - in that case i might want to have on disk storge and not in memory one.

To fully determine the whole database i need 1) template; 2) root nodes. Because I am using determenistic derivation of derived nodes, all other values can be calculated

For a given derived node I might want to know:
1) What nodes go before after (find future and history)


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






