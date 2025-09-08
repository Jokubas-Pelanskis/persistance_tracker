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
        name TEXT,
        type TEXT ,
        command TEXT NOT NULL DEFAULT '',
        UNIQUE(name, type, command)
    );

    CREATE TABLE IF NOT EXISTS template_edges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        connection_name TEXT,
        first INTEGER,
        second INTEGER,
        FOREIGN KEY (first) REFERENCES template_nodes(id),
        FOREIGN KEY (second) REFERENCES template_nodes(id),
        UNIQUE(first, second, connection_name)
    );
    CREATE TABLE IF NOT EXISTS nodes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        template_name INTEGER,
        FOREIGN KEY (template_name) REFERENCES template_nodes(id),
        UNIQUE(name, template_name)
    );
    CREATE TABLE IF NOT EXISTS edges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        connection_name TEXT,
        first INTEGER,
        second INTEGER,
        FOREIGN KEY (first) REFERENCES nodes(id),
        FOREIGN KEY (second) REFERENCES nodes(id),
        UNIQUE(first, second, connection_name)
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
                INSERT OR IGNORE INTO template_edges (first, second, connection_name)
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
                INSERT OR IGNORE INTO template_edges (first, second, connection_name)
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
        template_nodes_d = {c[0]:c[1] for c in template_nodes}

        # Get all calculations
        self.cursor.execute("SELECT id, name, type FROM template_nodes WHERE type = 'calculation'")
        calculation_nodes = self.cursor.fetchall()
        calculation_nodes_d = {c[0]: c[1] for c in calculation_nodes}

        # get all the template edges
        self.cursor.execute("SELECT id, first, second FROM template_edges")
        template_edges = self.cursor.fetchall()


        # Create a graph from nodes and edges
        import networkx as nx
        G = nx.DiGraph()
        G.add_nodes_from(list(map(lambda x: x[0], template_nodes)))
        G.add_edges_from(list(map(lambda x: (x[1], x[2]), template_edges)))

        maps = {}
        # transverse the graph from root to leaves
        for node in nx.topological_sort(G):
            print(node, G.in_edges(node), G.out_edges(node))
            # if it is a leaf node, then it is a root node
            if len(G.in_edges(node)) == 0:
                pass
            else:
                # find all the paths from root to this node
                pred = nx.ancestors(G, node)
                # all calculations
                all_c = pred.intersection(set(map(lambda x: x[0], calculation_nodes)))
                # find all root nodes
                root_nodes = [n for n in pred if len(G.in_edges(n)) == 0]

                self_name  = template_nodes_d[node]
                calcs = list(map(lambda i: calculation_nodes_d[i] ,all_c))
                roots =list(map(lambda i: template_nodes_d[i], root_nodes))

                name = self._generate_name(calcs, roots, self_name)
                maps[node] = name
                # Insert the node into the list of nodes
            
                self.cursor.execute(
                    "INSERT OR IGNORE INTO nodes (name, template_name) VALUES (?, ?)", (name, node)
                )
        
        # Create new calculation edges and insert them

        for edge in template_edges:
            first, second = edge[1], edge[2]
            if first in maps and second in maps:
                self.cursor.execute(
                    """
                    INSERT OR IGNORE INTO edges (first, second, connection_name)
                    SELECT n1.id, n2.id, ?
                    FROM nodes n1, nodes n2
                    WHERE n1.name = ? AND n2.name = ?
                    """,
                    (edge[2], maps[first], maps[second])
                )

        self.conn.commit()





    def get(self, names: List[str]):
        ...

    def select_pipes(self, template_names: List[str]):
        ...

    def select_history(self, node_id: int):
        ...

    def copy(self):
        ...

    def _generate_name(self, calcs: list[str], roots: list[str], self_name: str):
        """calculate a hash based on calculations and roots"""
        import hashlib

        text  = ""
        c = sorted(calcs)
        roots = sorted(roots)
        for i in roots:
            text += i
        for i in c:
            text +=i
        text +=  self_name
        
        h = md5_hash = hashlib.md5(text.encode("utf-8")).hexdigest()

        return h
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
        db.template_register_calculation(f"calci{i}", command=f"python3 program.py input(datai{i}) output(datai{i+1})")

    db.add_calculation()

    print("fisihed inserting")

    print(db.template_to_dot())

    print("---------------")
    print(db.to_dot())