#!/bin/bash
# Run ingestion scripts as modules from the package root

python -m graph_ingest.ingest_scripts.get_collections_cmr
python -m graph_ingest.ingest_scripts.ingest_node_dataset
python -m graph_ingest.ingest_scripts.ingest_node_datacenter
python -m graph_ingest.ingest_scripts.ingest_edge_datacenter_dataset
python -m graph_ingest.ingest_scripts.ingest_node_project
python -m graph_ingest.ingest_scripts.ingest_edge_dataset_project
python -m graph_ingest.ingest_scripts.ingest_node_platform
python -m graph_ingest.ingest_scripts.ingest_edge_dataset_platform
python -m graph_ingest.ingest_scripts.ingest_node_instrument
python -m graph_ingest.ingest_scripts.ingest_edge_platform_instrument
python -m graph_ingest.ingest_scripts.ingest_node_publication
python -m graph_ingest.ingest_scripts.ingest_edge_publication_dataset
python -m graph_ingest.ingest_scripts.ingest_node_edge_gcmd_sciencekeywords
python -m graph_ingest.ingest_scripts.ingest_edge_dataset_sciencekeyword
python -m graph_ingest.ingest_scripts.ingest_node_edge_publications_publications

# ### Machine Learning Based Script
python -m graph_ingest.ingest_scripts.ingest_edge_publication_applied_research_area
