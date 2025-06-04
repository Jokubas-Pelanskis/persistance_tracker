
/*
DNode - Only contains abstract calculations. real data are only described by location 

*/
use std::collections::BTreeMap;
use pyo3::prelude::*;
use serde::{Serialize, Deserialize};

/// Set types for interacting with the database
type IdCTemplate = String;
type IdDTemplate = String;
type IdC = u32;
type IdD = u32;
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
    pub incoming: Vec<DNodeTemplate>,
    pub outcoming: Vec<DNodeTemplate>
}

#[pyclass]
#[derive(Clone)]
enum NodeTemplate {
    Calculation(CNodeTemplate),
    Data(DNodeTemplate),
}


/// Describes abstract calculations.
#[pyclass]
pub struct DatabaseTemplate {
    id: IdTemplate,
    CNodes: BTreeMap<IdCTemplate, CNodeTemplate>, // Store all calculation nodes
    DNodes: BTreeMap<IdDTemplate, DNodeTemplate>, // Store all data nodes
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
    pub incoming: Vec<DNode>,
    pub outcoming: Vec<DNode>
}

#[pyclass]
#[derive(Clone)]
enum Node {
    Calculation(CNode),
    Data(DNode),
}



#[pymethods]
impl DNodeTemplate {
    fn __str__(&self) -> String {
        format!("'id': {}", self.id)
    }
}



#[pymethods]
impl DatabaseTemplate {

    #[new]
    pub fn new(name: IdCTemplate) -> Self {
        DatabaseTemplate {
            id : name,
            CNodes: BTreeMap::new(),
            DNodes: BTreeMap::new(),
        }

    }

    /// Register data node
    pub fn register_dnode(&mut self, name: String) -> DNodeTemplate {
        let dnode = DNodeTemplate {id: name.clone()};
        self.DNodes.insert(name, dnode.clone());
        dnode
    }

    /// Register calculation node
    pub fn register_cnode(&mut self, name: String, command: String) -> CNodeTemplate {
        let cnode = CNodeTemplate {id: name.clone()};
        self.CNodes.insert(name, dnode.clone());
        cnode
    }

    /// get a node from a template
    pub fn get(&self, name: String) -> NodeTemplate {
        unimplemented!();
    }

    /// Create a calculation
    pub fn create_calculation(&self, leafs: Vec<(String,String)>) -> Database {
        unimplemented!();
    }

}


/// Describes implementations and actual calculations
#[pyclass]
pub struct Database {

    template: DatabaseTemplate, // Store the template
    CNodes: BTreeMap<IdC, CNode>, // Store all calculation nodes
    DNodes: BTreeMap<IdD, DNode>, // Store all data nodes

}

/// Implement all selection and filtering functions
#[pymethods]
impl Database {
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


#[pymodule]
fn graphrlib_test(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Database>()?;
    m.add_class::<DatabaseTemplate>()?;
    Ok(())
}
