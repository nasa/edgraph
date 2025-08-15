# %%
import pandas as pd
import torch
from torch_geometric.data import Data
from neo4j import GraphDatabase
from torch_geometric.nn import GCNConv
import torch.nn.functional as F
import torch_geometric.transforms as T
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import roc_auc_score, accuracy_score, precision_score, recall_score
from tqdm import tqdm
import random

# Neo4j credentials
uri = "bolt://localhost:7687"
username = "neo4j"
password = ""

# Step 1: Connect to Neo4j and fetch data
def get_all_nodes(driver):
    query = "MATCH (n) RETURN id(n) AS node_id, labels(n)[0] AS node_type"
    with driver.session() as session:
        result = session.run(query)
        return [(record['node_id'], record['node_type']) for record in result]

def get_all_edges(driver):
    query = "MATCH (n)-[r]->(m) RETURN id(n) AS source, id(m) AS target"
    with driver.session() as session:
        result = session.run(query)
        return [(record['source'], record['target']) for record in result]

def fetch_neo4j_graph(uri, username, password):
    driver = GraphDatabase.driver(uri, auth=(username, password))
    nodes_data = get_all_nodes(driver)
    edges_data = get_all_edges(driver)
    driver.close()
    return nodes_data, edges_data

# Fetch graph data from Neo4j
nodes_data, edges_data = fetch_neo4j_graph(uri, username, password)

# Step 2: Process data into DataFrames
def process_nodes(nodes_data):
    node_ids = [node[0] for node in nodes_data]
    node_types = [node[1] for node in nodes_data]
    return pd.DataFrame({'node_id': node_ids, 'node_type': node_types})

def process_edges(edges_data):
    return pd.DataFrame(edges_data, columns=['source', 'target'])

# Process nodes and edges data
node_df = process_nodes(nodes_data)
edge_df = process_edges(edges_data)

# Function to filter the edges
def filter_edges_for_link_prediction(node_df, edge_df):
    dataset_nodes = node_df[node_df['node_type'] == 'Dataset']['node_id'].values
    sciencekeyword_nodes = node_df[node_df['node_type'] == 'ScienceKeyword']['node_id'].values

    filtered_edges = edge_df[
        (edge_df['source'].isin(dataset_nodes) & edge_df['target'].isin(sciencekeyword_nodes)) |
        (edge_df['source'].isin(sciencekeyword_nodes) & edge_df['target'].isin(dataset_nodes))
    ]
    return filtered_edges

# Filter edges
filtered_edges = filter_edges_for_link_prediction(node_df, edge_df)

# Function to create the Data object
def create_filtered_pyg_data(node_df, edge_df):
    node_type_mapping = {t: i for i, t in enumerate(node_df['node_type'].unique())}
    node_type_indices = node_df['node_type'].map(node_type_mapping).values
    num_node_types = len(node_type_mapping)
    node_features = torch.eye(num_node_types)[node_type_indices].float()
    edge_index = torch.tensor(edge_df.values, dtype=torch.long).t().contiguous()
    return Data(x=node_features, edge_index=edge_index)

# Create PyTorch Geometric data
pyg_data = create_filtered_pyg_data(node_df, filtered_edges)

# Function to create positive and negative edges for training
def create_filtered_link_prediction_data(data, filtered_edge_index):
    pos_edge_index = torch.tensor(filtered_edge_index.values, dtype=torch.long).t().contiguous()
    num_nodes = data.num_nodes
    neg_edge_index = torch.randint(0, num_nodes, pos_edge_index.size(), dtype=torch.long)
    return pos_edge_index, neg_edge_index

pos_edge_index, neg_edge_index = create_filtered_link_prediction_data(pyg_data, filtered_edges)

# New model definition
class LinkPredictionGCN(torch.nn.Module):
    def __init__(self, in_channels, hidden_channels):
        super(LinkPredictionGCN, self).__init__()
        self.conv1 = GCNConv(in_channels, hidden_channels)
        self.conv2 = GCNConv(hidden_channels, 1)

    def encode(self, x, edge_index):
        x = self.conv1(x, edge_index)
        x = F.relu(x)
        return self.conv2(x, edge_index)

    def decode(self, z, edge_index):
        return torch.sigmoid((z[edge_index[0]] * z[edge_index[1]]).sum(dim=1))

    def forward(self, data):
        z = self.encode(data.x, data.edge_index)
        return self.decode(z, data.edge_index)

# Function to create positive and negative edges for training
def create_link_prediction_data(data):
    pos_edge_index = data.edge_index
    num_nodes = data.num_nodes
    neg_edge_index = torch.randint(0, num_nodes, (2, pos_edge_index.size(1)), dtype=torch.long)
    return pos_edge_index, neg_edge_index

# Function to split edges into training and testing sets
def train_test_split_edges(edge_index, test_ratio=0.2):
    indices = np.arange(edge_index.size(1))
    np.random.shuffle(indices)
    test_size = int(test_ratio * len(indices))
    test_indices = indices[:test_size]
    train_indices = indices[test_size:]
    train_edge_index = edge_index[:, train_indices]
    test_edge_index = edge_index[:, test_indices]
    return train_edge_index, test_edge_index

# Function to generate negative edges
def generate_negative_edges(num_nodes, num_edges):
    return torch.randint(0, num_nodes, (2, num_edges), dtype=torch.long)

train_edge_index, test_edge_index = train_test_split_edges(pyg_data.edge_index)
train_neg_edge_index = generate_negative_edges(pyg_data.num_nodes, train_edge_index.size(1))
test_neg_edge_index = generate_negative_edges(pyg_data.num_nodes, test_edge_index.size(1))

# Training and evaluation functions
def train_and_evaluate_model(data, train_edge_index, train_neg_edge_index, test_edge_index, test_neg_edge_index):
    model = LinkPredictionGCN(data.num_features, hidden_channels=16)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
    loss_fn = torch.nn.BCELoss()

    train_loss = []
    train_auc = []
    train_accuracy = []
    train_precision = []
    train_recall = []

    for epoch in range(100):
        model.train()
        optimizer.zero_grad()
        z = model.encode(data.x, data.edge_index)
        train_pos_out = model.decode(z, train_edge_index)
        train_neg_out = model.decode(z, train_neg_edge_index)

        loss = loss_fn(train_pos_out, torch.ones(train_pos_out.size(0))) + loss_fn(train_neg_out, torch.zeros(train_neg_out.size(0)))
        loss.backward()
        optimizer.step()

        train_loss.append(loss.item())
        y_true = torch.cat([torch.ones(train_pos_out.size(0)), torch.zeros(train_neg_out.size(0))])
        y_pred = torch.cat([train_pos_out, train_neg_out]).detach().cpu().numpy()

        y_pred_binary = (y_pred > 0.5).astype(int)
        auc = roc_auc_score(y_true, y_pred)
        accuracy = accuracy_score(y_true, y_pred_binary)
        precision = precision_score(y_true, y_pred_binary)
        recall = recall_score(y_true, y_pred_binary)

        train_auc.append(auc)
        train_accuracy.append(accuracy)
        train_precision.append(precision)
        train_recall.append(recall)

        print(f"Epoch {epoch}, Loss: {loss.item()}, AUC: {auc}, Accuracy: {accuracy}, Precision: {precision}, Recall: {recall}")

    model.eval()
    with torch.no_grad():
        test_pos_out = model.decode(z, test_edge_index)
        test_neg_out = model.decode(z, test_neg_edge_index)

        y_true_test = torch.cat([torch.ones(test_pos_out.size(0)), torch.zeros(test_neg_out.size(0))])
        y_pred_test = torch.cat([test_pos_out, test_neg_out]).cpu().numpy()

        test_auc = roc_auc_score(y_true_test, y_pred_test)
        test_accuracy = accuracy_score(y_true_test, (y_pred_test > 0.5).astype(int))
        test_precision = precision_score(y_true_test, (y_pred_test > 0.5).astype(int))
        test_recall = recall_score(y_true_test, (y_pred_test > 0.5).astype(int))

        print(f"Test AUC: {test_auc}, Accuracy: {test_accuracy}, Precision: {test_precision}, Recall: {test_recall}")

    fig, ax1 = plt.subplots()
    ax1.plot(range(len(train_loss)), train_loss, 'g-', label='Loss')
    ax1.set_xlabel('Epochs')
    ax1.set_ylabel('Loss', color='g')

    ax2 = ax1.twinx()
    ax2.plot(range(len(train_auc)), train_auc, 'b-', label='Train AUC')
    ax2.set_ylabel('Metrics', color='b')
    fig.legend(loc='upper left')
    plt.show()

# Call the function
train_and_evaluate_model(pyg_data, train_edge_index, train_neg_edge_index, test_edge_index, test_neg_edge_index)

def extract_filtered_predicted_links(model, data, dataset_nodes, sciencekeyword_nodes, threshold=0.99, fraction=0.01):
    model.to("cuda")
    data.to("cuda")
    z = model.encode(data.x, data.edge_index)
    predicted_edges = []
    num_samples = int(fraction * len(dataset_nodes) * len(sciencekeyword_nodes))
    for _ in tqdm(range(num_samples), desc="Predicting Links"):
        dataset_node = random.choice(dataset_nodes)
        sciencekeyword_node = random.choice(sciencekeyword_nodes)
        score = torch.sigmoid((z[dataset_node] * z[sciencekeyword_node]).sum()).item()
        if score >= threshold:
            predicted_edges.append((dataset_node, sciencekeyword_node))
    return predicted_edges

# Assume `node_df` has a column `node_id` and `node_type`
dataset_nodes = node_df[node_df['node_type'] == 'Dataset']['node_id'].values
sciencekeyword_nodes = node_df[node_df['node_type'] == 'ScienceKeyword']['node_id'].values

# Use the function
predicted_links = extract_filtered_predicted_links(trained_model, pyg_data, dataset_nodes, sciencekeyword_nodes, threshold=0.5, fraction=0.01)

existing_edges = list(zip(pyg_data.edge_index[0].tolist(), pyg_data.edge_index[1].tolist()))
new_links = [link for link in predicted_links if link not in existing_edges]
print(f'The total number of new links created is {len(new_links)}')

# Connect to Neo4j
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "test"))

# Insert new predicted links
def insert_predicted_links(driver, new_links):
    with driver.session() as session:
        for src, dst in new_links:
            query = """
            MATCH (a), (b)
            WHERE id(a) = $src AND id(b) = $dst
            CREATE (a)-[:PREDICTED_DATASET_SCIENCEKEYWORD]->(b)
            """
            session.run(query, src=src, dst=dst)

insert_predicted_links(driver, new_links)
driver.close()
