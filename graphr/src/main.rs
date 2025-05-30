
use clap::{Parser,Subcommand};
use std::collections::BTreeMap;
use petgraph::dot::{Dot, Config};
use graphrlib::*;
const JSONDATABASE: &str  = ".graph/graph.json";
const CURRENTTAGS: &str  = ".graph/current_tags.json";
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