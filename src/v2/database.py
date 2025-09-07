"""
SQL database schema:


    -- Create template names

    CREATE TABLE IF NOT EXISTS template_nodes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
    );

    CREATE TABLE IF NOT EXISTS template_calculation_nodes (
        id INT PRIMARY KEY REFERENCES data_common(id) ON DELETE CASCADE,
    );
    CREATE TABLE IF NOT EXISTS template_data_nodes (
        id INT PRIMARY KEY REFERENCES data_common(id) ON DELETE CASCADE,
        command TEXT
    );

    CREATE TABLE IF NOT EXISTS template_edges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        connection_name TEXT,
        FOREIGN KEY (first) REFERENCES tempalte_nodes(id),
        FOREIGN KEY (second) REFERENCES template_nodes(id)
    );

    -- Create instances

    CREATE TABLE IF NOT EXISTS nodes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        FOREIGN KEY (template_name) REFERENCES template_nodes(id)
    );


    CREATE TABLE IF NOT EXISTS edges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        connection_name TEXT,
        FOREIGN KEY (first) REFERENCES nodes(id),
        FOREIGN KEY (second) REFERENCES nodes(id)

    );

    CREATE TABLE IF NOT EXISTS extra_calculation_parameters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        FOREIGN KEY (calculation_id) REFERENCES calculation_nodes(id)
        extra_name TEXT,
        extra_value TEXT,
    );

    -- store sessions

    CREATE TABLE IF NOT EXISTS sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_time TEXT,
    );

    CREATE TABLE IF NOT EXISTS sessions_nodes {
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        FOREIGN KEY (session_id) REFERENCES sessions(id),
        FOREIGN KEY (node_id) REFERENCES nodes(id)
    };


    CREATE TABLE IF NOT EXISTS template_template_nodes {
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        FOREIGN KEY (session_id) REFERENCES sessions(id),
        FOREIGN KEY (node_id) REFERENCES template_nodes(id)
    };


Example of usage:
# I want  to be able to add calculations and then filter calculations too.
# Saving everything to the database

# every
db = gl.Database(id = calculation_id) # do not do that becasue that adds restriction. (i cannot construct multiple calculations at the same time)
db = gl.Database() # do this and then add calculation-id 
# When do you generate the id? What is id? What does it identify: a specific calculation? An instance of a calculation

# generate a new id for every run (new id, when I come back)
for root_name in root_names:
    db.add_calculation(root_name)

# now I want to modify the caluclations (say add extra or change the parameter if somtehing has crashed)
for new_root_name in root_names_new:
    db.add_calculation(new_root_name)



# get an already constructed calculation (without being able to change it); but add a copy functionality to to create a database that you can modify. That allso creates a new id.
db = gl.get_database([calculation_id]) # if None is provided, then it connects to the whole databsae. If a list is provided, then multiple datbases are connected together.

# copy the database, this creates a database
new_db = db.copy()

# Generate an empty calculation to be built
db, calculation_id = gl.create_empty()

db.template_register_dnode()
db.template_register_cnode()

db.template_select_history("template_name") # find all nodes that do not fit the filter and then remove them from the template 


db.add_calculation(...) # for the template calculete all the calculation node names.

# querying and finding information in the database
names = db.get(['name1', 'name2']) # returns a table.

# Now I can do whatever I want with that. (Could write helper functions to convert to different formats.) and transform the data

new_db = gl.create_from_subset(names) # takes filtered dataset and creates a database


"""

import sqlite3
from typing import Any, Dict, List, Optional, Tuple
import re
DB_PATH = "persistance_tracker.db"

def _init_db(conn: sqlite3.Connection):
    c = conn.cursor()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS template_nodes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        type TEXT,
        command TEXT NULL
    );

    CREATE TABLE IF NOT EXISTS template_edges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        connection_name TEXT,
        first INTEGER,
        second INTEGER,
        FOREIGN KEY (first) REFERENCES template_nodes(id),
        FOREIGN KEY (second) REFERENCES template_nodes(id)
    );
    CREATE TABLE IF NOT EXISTS nodes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        template_name INTEGER,
        FOREIGN KEY (template_name) REFERENCES template_nodes(id)
    );
    CREATE TABLE IF NOT EXISTS edges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        connection_name TEXT,
        first INTEGER,
        second INTEGER,
        FOREIGN KEY (first) REFERENCES nodes(id),
        FOREIGN KEY (second) REFERENCES nodes(id)
    );
    CREATE TABLE IF NOT EXISTS extra_calculation_parameters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        calculation_id INTEGER,
        extra_name TEXT,
        extra_value TEXT,
        FOREIGN KEY (calculation_id) REFERENCES nodes(id)
    );
    CREATE TABLE IF NOT EXISTS sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_time TEXT
    );
    CREATE TABLE IF NOT EXISTS sessions_nodes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id INTEGER,
        node_id INTEGER,
        FOREIGN KEY (session_id) REFERENCES sessions(id),
        FOREIGN KEY (node_id) REFERENCES nodes(id)
    );

    -- create indexes
    CREATE INDEX IF NOT EXISTS idx_sessions_nodes_session_id ON sessions_nodes(session_id);
    CREATE INDEX IF NOT EXISTS idx_sessions_nodes_node_id ON sessions_nodes(node_id);
    CREATE INDEX IF NOT EXISTS idx_nodes_template_name ON nodes(template_name);
    CREATE INDEX IF NOT EXISTS idx_edges_first ON edges(first);
    CREATE INDEX IF NOT EXISTS idx_edges_second ON edges(second);
    CREATE INDEX IF NOT EXISTS idx_template_edges_first ON template_edges(first);
    CREATE INDEX IF NOT EXISTS idx_template_edges_second ON template_edges(second);
    CREATE INDEX IF NOT EXISTS idx_extra_calculation_parameters_calculation_id ON extra_calculation_parameters(calculation_id);
    CREATE INDEX IF NOT EXISTS idx_template_nodes_name ON template_nodes(name);
    CREATE INDEX IF NOT EXISTS idx_nodes_name ON nodes(name);
    CREATE INDEX IF NOT EXISTS idx_sessions_session_time ON sessions(session_time);
    CREATE INDEX IF NOT EXISTS idx_extra_calculation_parameters_extra_name ON extra_calculation_parameters(extra_name);
    CREATE INDEX IF NOT EXISTS idx_extra_calculation_parameters_extra_value ON extra_calculation_parameters(extra_value);
    CREATE INDEX IF NOT EXISTS idx_template_nodes_type ON template_nodes(type);
    CREATE INDEX IF NOT EXISTS idx_template_nodes_command ON template_nodes(command);
    


    """)
    conn.commit()

class Database:
    def __init__(self, session_ids: Optional[List[int]] = None, read_only: bool = True):
        self.conn = sqlite3.connect(DB_PATH)
        _init_db(self.conn)
        self.read_only = read_only
        self.session_ids = session_ids or []
        self.cursor = self.conn.cursor()


    def template_register_calculation(self, name: str,command = str):
        self.cursor.execute(
            "INSERT OR IGNORE INTO template_nodes (name, type) VALUES (?, ?)", (name, "calculation")
        )

        # extract all values under 'input(...)'
        inputs = re.findall(r'input\((.*?)\)', command)
        for input_name in inputs:
            self.cursor.execute(
                "INSERT OR IGNORE INTO template_nodes (name, type) VALUES (?, ?)", (input_name, "data")
            )
            # Add edge from input data node to calculation node
            self.cursor.execute(
                """
                INSERT INTO template_edges (first, second, connection_name)
                SELECT t1.id, t2.id, ?
                FROM template_nodes t1, template_nodes t2
                WHERE t1.name = ? AND t2.name = ?
                """,
                (f"input_{input_name}", input_name, name)
            )
        # extract all values under 'output(...)'
        outputs = re.findall(r'output\((.*?)\)', command)
        for output_name in outputs:
            self.cursor.execute(
                "INSERT OR IGNORE INTO template_nodes (name, type) VALUES (?, ?)", (output_name, "data")
            )
            # Add edge from calculation node to output data node
            self.cursor.execute(
                """
                INSERT INTO template_edges (first, second, connection_name)
                SELECT t1.id, t2.id, ?
                FROM template_nodes t1, template_nodes t2
                WHERE t1.name = ? AND t2.name = ?
                """,
                (f"output_{output_name}", name, output_name)
            )


        # TODO: add sessions
        self.conn.commit()

    def template_select_history(self, template_name: str):
        ...

    def add_calculation(self):
        """
        Add a new calculation to the database based on the current template, which is determined by the session id
        """
        if self.read_only:
            raise ValueError("Database is read-only. Cannot add calculation.")
        
        # For the current template, go through all template nodes. for each node find all the root nodes and all intermediate calculations, then construct a dictionary, pass that to _calculate_hash and then instert the result under that name into nodes

        # Given the template, which nodes and edges describe a digprah, find the root nodes and all calculations
        self.cursor.execute("SELECT id, name, type FROM template_nodes")
        template_nodes = self.cursor.fetchall()

        # Get all 
        self.cursor.execute("SELECT id, name, type FROM template_nodes WHERE type = 'calculation'")
        calculation_nodes = self.cursor.fetchall()

        # get all the template edges
        self.cursor.execute("SELECT id, first, second FROM template_edges")
        template_edges = self.cursor.fetchall()

        # 
        self.cursor.execute("SELECT second, COUNT(*) from template_edges group by second")
        x = self.cursor.fetchall()

        print(x)
        print("---")


        print(calculation_nodes)
        print('....')
        print(template_nodes)
        print(template_edges)


    def get(self, names: List[str]):
        ...

    def select_pipes(self, template_names: List[str]):
        ...

    def select_history(self, node_id: int):
        ...

    def copy(self):
        ...


    def template_to_dot(self) -> str:
        """Convert the template to DOT format for visualization."""
        dot = ["digraph G {"]
        # Add nodes
        self.cursor.execute("SELECT id, name FROM template_nodes")
        for node_id, name in self.cursor.fetchall():
            dot.append(f'    "{node_id}" [label="{name}"];')
        # Add edges
        self.cursor.execute("SELECT first, second, connection_name FROM template_edges")
        for first, second, connection_name in self.cursor.fetchall():
            label = f' [label="{connection_name}"]' if connection_name else ""
            dot.append(f'    "{first}" -> "{second}"{label};')
        dot.append("}")
        return "\n".join(dot)

    def to_dot(self) -> str:
        """Convert the whole database to DOT format for visualization."""
        dot = ["digraph G {"]
        # Add nodes
        self.cursor.execute("SELECT id, name FROM nodes")
        for node_id, name in self.cursor.fetchall():
            dot.append(f'    "{node_id}" [label="{name}"];')
        # Add edges
        self.cursor.execute("SELECT first, second, connection_name FROM edges")
        for first, second, connection_name in self.cursor.fetchall():
            label = f' [label="{connection_name}"]' if connection_name else ""
            dot.append(f'    "{first}" -> "{second}"{label};')
        dot.append("}")
        return "\n".join(dot)


def connect(session_ids: List[int]) -> Database:
    return Database(session_ids=session_ids, read_only=True)

def create_empty() -> Tuple[Database, int]:
    db = Database(read_only=False)
    db.cursor.execute("INSERT INTO sessions (session_time) VALUES (datetime('now'))")
    session_id = db.cursor.lastrowid
    db.conn.commit()
    return db, session_id


if __name__ == "__main__":

    db = Database(read_only= False)

    for i in range(4):
        db.template_register_calculation(f"calc{i}", command=f"python3 program.py input(data{i}) output(data{i+1})")

    db.add_calculation()

    # print("fisihed inserting")
    # print(db.template_to_dot())
