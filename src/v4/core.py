from __future__ import annotations
import sqlite3
import pathlib
import re
from typing import Any, Dict, List, Optional, Tuple
import re
import networkx as nx
DB_PATH = "persistance_tracker.db"

def _init_db(conn: sqlite3.Connection):
    c = conn.cursor()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS nodes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        type TEXT, -- tem_dat, tem_cal, tem_group, ins_dat, ins_cal, ins_group
        extra TEXT, -- JSON for any extra data
        UNIQUE(name, type)
    );
    CREATE TABLE IF NOT EXISTS edges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first INTEGER,
        second INTEGER,
        FOREIGN KEY (first) REFERENCES nodes(id),
        FOREIGN KEY (second) REFERENCES nodes(id),
        UNIQUE(first, second)
    );

    """)
    conn.commit()


class TemplateGroup:
    """Wraps around a set of templates nodes and edges."""

    def __init__(self, database: Database):
        self.database = database
        self.graph: Optional[nx.DiGraph] = nx.DiGraph() # Stores the template as graph.
        self.hash : Optional[str] = None

    def register(self, name: str, command: str):
        """
        Given the name and the command of the template it creates template nodes
        What it does:
        - Extract inputs and outputs using regex
        - Create calculation nodes and all relevant data nodes. and insert into the database.
        """
        # extract inputs, outputs and the command
        inputs = re.findall(r'input\((.*?)\)', command)
        outputs = re.findall(r'output\((.*?)\)', command)

        # add the template calculation node. (add calculation node as json)
        self.database.cursor.execute(
            "INSERT OR IGNORE INTO nodes (name, type, extra) VALUES (?, ?, ?)",
            (name, "tem_cal", '{"command":' + command + '}' )
        )
        
        # Insert inputs and outputs
        for inp in inputs:
            self.database.cursor.execute(
                "INSERT OR IGNORE INTO nodes (name, type) VALUES (?, ?)",
                (inp, "tem_dat")
            )
            # create edge from input to calculation
            self.database.cursor.execute(
                """
                INSERT OR IGNORE INTO edges (first, second)
                VALUES ((SELECT id FROM nodes WHERE name = ? AND type = 'tem_dat'),
                    (SELECT id FROM nodes WHERE name = ? AND type = 'tem_cal')
                )
                """,
                (inp, name)
            )
        for out in outputs:
            self.database.cursor.execute(
                "INSERT OR IGNORE INTO nodes (name, type) VALUES (?, ?)",
                (out, "tem_dat")
            )
            # create edge from calculation to output
            self.database.cursor.execute(
                """
                INSERT OR IGNORE INTO edges (first, second)
                VALUES ((SELECT id FROM nodes WHERE name = ? AND type = 'tem_cal'),
                    (SELECT id FROM nodes WHERE name = ? AND type = 'tem_dat')
                )
                """,
                (name, out)
            )
        
        # add to the graph (for in-memory representation and used to calculate the hash.)
        self.graph.add_node(name, type="tem_cal", command=command)
        for inp in inputs:
            self.graph.add_node(inp, type="tem_dat")
            self.graph.add_edge(inp, name)
        for out in outputs:
            self.graph.add_node(out, type="tem_dat")
            self.graph.add_edge(name, out)

    def commit(self) -> str:
        """
        Commit the template group to the database.
        Add tem_group node and 
        """
        if self.graph is None:
            raise ValueError("In memory graph is not defined. Either it was never created or already committed.")

        # Create a unique hash for the template group
        group_hash = self._hash()
        # Insert the tem_group node
        self.database.cursor.execute(
            "INSERT OR IGNORE INTO nodes (name, type, extra) VALUES (?, ?, ?)",
            (group_hash, "tem_group", None)
        )
        # Insert edges between group and all template nodes
        for node in self.graph.nodes:
            self.database.cursor.execute(
                """
                INSERT OR IGNORE INTO edges (first, second)
                VALUES (
                    (SELECT id FROM nodes WHERE name = ? AND type = 'tem_group'),
                    (SELECT id FROM nodes WHERE name = ?)
                )
                """,
                (group_hash, node)
            )

        self.database.conn.commit()
        self.graph = None
        self.hash = group_hash
        return group_hash


    def _hash(self) -> str:
        """Create a unique hash for the template group."""
        import hashlib
        # Create a sorted representation of the graph
        nodes = sorted((n, self.graph.nodes[n]['type'], self.graph.nodes[n].get('command', '')) for n in self.graph.nodes)
        edges = sorted((u, v) for u, v in self.graph.edges)
        representation = str(nodes) + str(edges)
        return hashlib.sha256(representation.encode()).hexdigest()


    def graph_dot(self) -> str:
        """A method for testing before committing. To make sure the graph is correct."""
        return nx.nx_pydot.to_pydot(self.graph).to_string()

    def as_dot(self) -> str:
        """Reconstruct the graph from the database."""
        if self.hash is None:
            raise ValueError("Hash is not defined.")

        if self.graph is not None:
            raise ValueError("Graph is still in memory. Please commit first.")
        
        # Query the database to reconstruct the graph
        dot = "digraph G {\n"
        # Get all nodes in the group
        self.database.cursor.execute(
            """
            SELECT n.id, n.name, n.type, n.extra
            FROM nodes n
            JOIN edges e ON n.id = e.second
            WHERE e.first = (SELECT id FROM nodes WHERE name = ? AND type = 'tem_group')
            """,
            (self.hash,)
        )
        nodes = self.database.cursor.fetchall()
        node_dict = {}
        for node_id, name, ntype, extra in nodes:
            label = f"{name}"

            dot += f'  {node_id} [label="{label}"];\n'
            node_dict[node_id] = name
        # Get all edges between these nodes
        self.database.cursor.execute(
            """
            SELECT e.first, e.second
            FROM edges e
            WHERE e.first IN (
                SELECT n.id
                FROM nodes n
                JOIN edges e2 ON n.id = e2.second
                WHERE e2.first = (SELECT id FROM nodes WHERE name = ? AND type = 'tem_group')
            )
            AND e.second IN (
                SELECT n.id
                FROM nodes n
                JOIN edges e2 ON n.id = e2.second
                WHERE e2.first = (SELECT id FROM nodes WHERE name = ? AND type = 'tem_group')
            )
            """,
            (self.hash, self.hash)
        )
        edges = self.database.cursor.fetchall()
        for first, second in edges:
            dot += f'  {first} -> {second};\n'
        dot += "}\n"
        return dot

class instanceGroup:
    """Wraps around a set of instance nodes and edges."""
    
    def __init__(self, database: Database):
        self.database = database
        self.graph: Optional[nx.DiGraph] = nx.DiGraph() # Stores
        self.hash : Optional[str] = None

    def register(self, template_group_name: str, leafs: dict[str, str]):
        
        """
        Given the template group name (a pipeline that I want to calculate) and the leafs (input data nodes) it creates instance nodes. Node names are calculated based on root nodes and intermediate calculation nodes.
        """
        # Get the template group from the database (from edges where first is the template with the id and )
        self.database.cursor.execute(
            """
            SELECT n2.id, n2.name
            FROM nodes n1
            JOIN edges e ON n1.id = e.first
            JOIN nodes n2 ON e.second = n2.id
            WHERE n1.name = ? AND n1.type = 'tem_group'
            """,
            (template_group_name,)
        )
        template_nodes = self.database.cursor.fetchall()

        # select all edges 
        print(template_nodes)


        # Go through all the template nodes in topological order and for each find it's root nodes and intermediate calculations. Based on these calculate hash.



    def register_instance_group(self, instance_group: instanceGroup):
        pass

    def delete_and_commit(self):
        pass

    def commit(self) -> str:
        pass

    def to_bash(self) -> str:
        pass

    def filter_template(self, template_names: list[str]):
        pass

    def __str__(self):
        pass



class Database:

    def __init__(self, database_path: str | pathlib.Path):
        """Connect to the database. This objcet provides a pathway to all data."""
        self.conn = sqlite3.connect(database_path)
        _init_db(self.conn)
        self.cursor = self.conn.cursor()

    def new_template_group(self) -> TemplateGroup:
        return TemplateGroup(self)

    def new_instance_group(self) -> instanceGroup:
        return instanceGroup(self)

    def get_instance_group(self, instance_group_id: list[str] | str) -> instanceGroup:
        pass


    



if __name__ == "__main__":
    # connect to the database
    db = Database("persistance_tracker.db")


    tg = db.new_template_group()
    for i in range(20):
        tg.register(f"calc{i}",f"python3 script.py input(data{i}) output(data{i+1})")

    tg_name = tg.commit()

    # now create some calculations
    cg = db.new_instance_group()
    for i in range(4):
        cg.register(template_group_name = tg_name, leafs = {"data0": f"start{i}"})

    cg_name = cg.commit()
    print(cg_name)



    # break
    # tg = db.new_template_group()
    # tg.register("calc1","python3 script.py input(myinput1) output(myoutput1)")
    # template_id = tg.commit() # commit this template. So that multiple could be used as one.

    # cg = db.new_instance_group()
    # cg.register(template_id = template_id, {"myinput1": "hi", "myoutput1": "bye"})
    # cg.register(template_id = template_id, {"myinput1": "hi", "myoutput1": "bye"})
    # instance_id = cg.commit()


    # # Another example, now I want to run a Bayesion optimization loop
    # ## Create a template

    # db = Database() # Connect to the datbasae
    # # Create a group of templates - this wil be a template for a workflow
    # tg = db.new_template_group()
    # tg.register("calc1","python3 script.py input(myinput1) output(myoutput1)")
    # tg.register("calc2","python3 script2.py input(myinput2) output(myoutput2)")
    # tg.register("optimizer","python3 optimizer.py input(optinput) output(optoutput)")
    # template_id = tg.commit() # commit this template. So that multiple could be used as one.

    # # Create a cluster of instances
    # cg1 = db.new_instance_group()
    # for i in range(4):
    #     # I would like to get a instance so that I could run only that one if I want to.
    #     cg2 = db.new_instance_group()
    #     cg2.register(template_id = template_id, {"optinput": "start"})
    #     cg_name = cg2.commit()
        
    #     script = cg2.to_bash() # convert to a basch script (could have other formats)
    #     run_slurm(script)

    #     # with each iteraciton cg1 changes, so I need to delete the old one.
    #     cg1.register_instance_group(cg2)
    #     cg1.delete_and_commit() 

    # # In this case just overwrite what I have done iteratively.
    # cg_name = cg1.commit()


    # # In some other program
    # db = Database() # Connect to the datbasae
    # cg = db.get_instance_group(cg_name)
    # cg.filter_template(["a", "b"]) # select only these templates
    # print(cg)
    
