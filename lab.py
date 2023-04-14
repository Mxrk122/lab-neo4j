from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable

class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def create_friendship(self, person1_name, person2_name):
        with self.driver.session(database="neo4j") as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.execute_write(
                self._create_and_return_friendship, person1_name, person2_name)
            for row in result:
                print("Created friendship between: {p1}, {p2}".format(p1=row['p1'], p2=row['p2']))

    @staticmethod
    def _create_and_return_friendship(tx, person1_name, person2_name):
        # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
        # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
        query = (
            "CREATE (p1:Person { name: $person1_name }) "
            "CREATE (p2:Person { name: $person2_name }) "
            "CREATE (p1)-[:KNOWS]->(p2) "
            "RETURN p1, p2"
        )
        result = tx.run(query, person1_name=person1_name, person2_name=person2_name)
        try:
            return [{"p1": row["p1"]["name"], "p2": row["p2"]["name"]}
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def find_person(self, person_name):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_person, person_name)
            for row in result:
                print("Found person: {row}".format(row=row))

    @staticmethod
    def _find_and_return_person(tx, person_name):
        query = (
            "MATCH (p:Person) "
            "WHERE p.name = $person_name "
            "RETURN p.name AS name"
        )
        result = tx.run(query, person_name=person_name)
        return [row["name"] for row in result]
    
    # PArte A
    
    # crea un nodo a partir de un label y un diccioanrio de propiedades
    def create_node(self, label, props):
        with self.driver.session(database="neo4j") as session:
            query = "CREATE (n:"+ label + "{"
            if props:
                for key in props:
                    value = props[key]
                    query += f" {key}: {value},"
                # eliminar la coma del final
                query = query[:len(query)-1]
                query += "}) RETURN n"
            result = session.run(query)
            return result
    
    # crea una relacion segun el laabel y una listaa que
    # contiene la propiedad y su valor de cada nodo
    # esta relacion se crea con un tipo y un diccionario de propiedades
    def create_relationship(self, node_label1, node_props1, rel_type, rel_props, node_label2, node_props2):
        with self.driver.session(database="neo4j") as session:
            query = (
                "MATCH (n1:"+ node_label1 +" {" + node_props1[0] +":" + node_props1[1] + "}) "
                "MATCH (n2:"+ node_label2 +" {" + node_props2[0] +":" + node_props2[1] + "}) "
                "MERGE (n1)-[r:"+ rel_type +" {"
            )
            if rel_props:
                for key in rel_props:
                    value = rel_props[key]
                    query += f" {key}: {value},"
                # eliminar la coma del final
                query = query[:len(query)-1]
            query += "}]->(n2) RETURN r"
            result = session.run(query)
            return result



if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    uri = "neo4j+s://1eac3519.databases.neo4j.io:7687"
    user = "neo4j"
    password = "oETZntR7Cg3b100cEWiat8UmEWk_zZfpLZ9f4Ja7lSM"
    app = App(uri, user, password)
    # PArte A
    USER = {
        "name" : "'darrel'",
        "userId" : "'1'"
    }

    MOVIE = {
        "title" : "'be quiet and drive'",
        "movieId" : 1,
        "year" : 2000,
        "plot" : "'que rolon'"
    }

    node1 = ["name", "'darrel'"]
    node2 = ["title", "'be quiet and drive'"]
    rel_props = {
        "rating" : 3,
        "timestamp" : 1925
    }

    app.create_node("user", USER)
    app.create_node("movie", MOVIE)
    app.create_relationship("user", node1, "rated", rel_props, "movie", node2)

    # PARte B
    USER = {
        "name" : "'Alice'",
        "userId" : "'2'"
    }

    MOVIE = {
            "title" : "'The Shawshank Redemption'",
            "movieId" : 2,
            "year" : 1994,
            "plot" : "'Two imprisoned men bond over a number of years, finding solace and eventual redemption through acts of common decency.'"
        }

    node1 = ["name", "'Alice'"]
    node2 = ["title", "'The Shawshank Redemption'"]
    rel_props = {
            "rating" : 4,
            "timestamp" : 1234567890
        }

    app.create_node("user", USER)
    app.create_node("movie", MOVIE)
    app.create_relationship("user", node1, "rated", rel_props, "movie", node2)

    USER = {
        "name" : "'Bob'",
        "userId" : "'3'"
    }

    MOVIE = {
            "title" : "'The Godfather'",
            "movieId" : 3,
            "year" : 1972,
            "plot" : "'An organized crime dynastys aging patriarch transfers control of his clandestine empire to his reluctant son'"
        }

    node1 = ["name", "'Bob'"]
    node2 = ["title", "'The Godfather'"]
    rel_props = {
            "rating" : 5,
            "timestamp" : 2345678901
        }

    app.create_node("user", USER)
    app.create_node("movie", MOVIE)
    app.create_relationship("user", node1, "rated", rel_props, "movie", node2)

    USER = {
        "name" : "'Charlie'",
        "userId" : "'4'"
    }

    MOVIE = {
            "title" : "'The Dark Knight'",
            "movieId" : 4,
            "year" : 2008,
            "plot" : "'When the menace known as the Joker wreaks havoc and chaos on the people of Gotham, Batman must accept one of the greatest psychological and physical tests of his ability to fight injustice.'"
        }

    node1 = ["name", "'Charlie'"]
    node2 = ["title", "'The Dark Knight'"]
    rel_props = {
            "rating" : 5,
            "timestamp" : 3456789012
    }

    app.create_node("user", USER)
    app.create_node("movie", MOVIE)
    app.create_relationship("user", node1, "rated", rel_props, "movie", node2)

    USER = {
        "name" : "'Dave'",
        "userId" : "'5'"
    }

    MOVIE = {
            "title" : "'Pulp Fiction'",
            "movieId" : 5,
            "year" : 1994,
            "plot" : "'The lives of two mob hitmen, a boxer, a gangster and his wife, and a pair of diner bandits intertwine in four tales of violence and redemption.'"
        }

    node1 = ["name", "'Dave'"]
    node2 = ["title", "'Pulp Fiction'"]
    rel_props = {
            "rating" : 4,
            "timestamp" : 4567890123
        }

    app.create_node("user", USER)
    app.create_node("movie", MOVIE)
    app.create_relationship("user", node1, "rated", rel_props, "movie", node2)

    # crear relaciones restantes
    node1 = ["name", "'Alice'"]
    node2 = ["title", "'Pulp Fiction'"]
    rel_props = {
            "rating" : 1,
            "timestamp" : 4564590123
        }
    app.create_relationship("user", node1, "rated", rel_props, "movie", node2)

    node1 = ["name", "'Charlie'"]
    node2 = ["title", "'Pulp Fiction'"]
    rel_props = {
            "rating" : 5,
            "timestamp" : 4564590123
        }
    app.create_relationship("user", node1, "rated", rel_props, "movie", node2)

    node1 = ["name", "'Dave'"]
    node2 = ["title", "'be quiet and drive'"]
    rel_props = {
            "rating" : 5,
            "timestamp" : 4564590123
        }
    app.create_relationship("user", node1, "rated", rel_props, "movie", node2)

    node1 = ["name", "'darrel'"]
    node2 = ["title", "'The Godfather'"]
    rel_props = {
            "rating" : 1,
            "timestamp" : 4564324123
        }
    app.create_relationship("user", node1, "rated", rel_props, "movie", node2)

    node1 = ["name", "'Bob'"]
    node2 = ["title", "'The Dark Knight'"]
    rel_props = {
            "rating" : 1,
            "timestamp" : 4564324123
        }
    app.create_relationship("user", node1, "rated", rel_props, "movie", node2)
        
    app.close()