"""
Generated test fixtures based on real sample data.
This file contains example data for each node and edge type in the EOSDIS Knowledge Graph.
"""

# Generated from sample data files

# Node Type Examples
MOCK_DATASET = {   'identity': 1087,
    'labels': ['Dataset'],
    'properties': {   'abstract': 'Nitrogen dioxide Level 3 files provide trace gas information on '
                                  'a regular grid covering the TEMPO field of regard for nominal '
                                  'TEMPO observations. Level 3 files are derived by combining '
                                  'information from all Level 2 files constituting a TEMPO '
                                  'East-West scan cycle. The files are provided in netCDF4 format, '
                                  'and contain information on tropospheric, stratospheric and '
                                  'total nitrogen dioxide vertical columns, ancillary data used in '
                                  'air mass factor and stratospheric/tropospheric separation '
                                  'calculations, and retrieval quality flags. The re-gridding '
                                  'algorithm uses an area-weighted approach. These data reached '
                                  'provisional validation on December 9, 2024.',
                      'cmrId': 'C2930763263-LARC_CLOUD',
                      'daac': 'NASA/LARC/SD/ASDC',
                      'doi': '10.5067/IS-40e/TEMPO/NO2_L3.003',
                      'globalId': '065f4b3f-8b3c-5098-aa9b-6558c1570432',
                      'longName': 'TEMPO gridded NO2 tropospheric and stratospheric columns V03 '
                                  '(PROVISIONAL)',
                      'pagerank_publication_dataset': 0.15000000000000002,
                      'shortName': 'TEMPO_NO2_L3',
                      'temporalExtentEnd': '',
                      'temporalExtentStart': '2023-08-01T00:00:00.000Z',
                      'temporalFrequency': 'Unknown'}}

MOCK_PROJECT = {   'identity': 5817,
    'labels': ['Project'],
    'properties': {   'globalId': 'a7af033b-4ea3-52d5-a097-126c2f91d824',
                      'longName': 'Commercial Smallsat Data Acquisition Program',
                      'shortName': 'CSDA'}}

MOCK_DATACENTER = {   'identity': 1951,
    'labels': ['DataCenter'],
    'properties': {   'globalId': '46b38de5-bd0d-5055-a829-27b9bd736e7a',
                      'longName': 'N/A',
                      'shortName': 'BYU/SCP',
                      'url': 'N/A'}}

MOCK_INSTRUMENT = {   'identity': 6844,
    'labels': ['Instrument'],
    'properties': {   'globalId': 'eeef1800-d960-5be6-821b-2ba4be66cc69',
                      'longName': 'Optical Spectrometer',
                      'shortName': 'OPTSPEC'}}

MOCK_PUBLICATION = {   'identity': 20000,
    'labels': ['Publication'],
    'properties': {   'abstract': 'Antarctic continental shelf waters are the most biologically '
                                  'productive in the Southern Ocean. Although satellite-derived '
                                  'algorithms report peak productivity during the austral '
                                  'spring/early summer, recent studies provide evidence for '
                                  'substantial late summer productivity that is associated with '
                                  'green colored frazil ice. Here we analyze daily Moderate '
                                  'Resolution Imaging Spectroradiometer satellite images for '
                                  'February and March from 2003 to 2017 to identify green colored '
                                  'frazil ice hot spots. Green frazil ice is concentrated in 11 of '
                                  'the 13 major sea ice production polynyas, with the greenest '
                                  'frazil ice in the Terra Nova Bay and Cape Darnley polynyas. '
                                  'While there is substantial interannual variability, green '
                                  'frazil ice is present over greater than 300,000 km2 during '
                                  'March. Late summer frazil ice-associated algal productivity may '
                                  'be a major phenomenon around Antarctica that is not considered '
                                  'in regional carbon and ecosystem models.',
                      'authors': '',
                      'doi': '10.1002/2017GL075472',
                      'globalId': '9b84e063-baa9-58ef-9131-e6888f17a66b',
                      'pagerank_publication_dataset': 0.5520234375,
                      'title': 'Late Summer Frazil IceAssociated Algal Blooms around Antarctica',
                      'year': '2018'}}

MOCK_PLATFORM = {   'identity': 6120,
    'labels': ['Platform'],
    'properties': {   'Type': 'Earth Observation Satellites',
                      'globalId': '5970954c-9ad7-50e9-b42d-2a962b98db72',
                      'longName': 'Geostationary Operational Environmental Satellite 13',
                      'shortName': 'GOES-13'}}

MOCK_SCIENCEKEYWORD = {   'identity': 31622,
    'labels': ['ScienceKeyword'],
    'properties': {   'globalId': '407c1355-4797-5a1c-8982-f6dbe7a56e6a',
                      'name': 'SEDIMENTARY TEXTURES'}}

# Edge Type Examples
MOCK_EDGE_HAS_DATASET = {   'relationship': {   'elementId': '5:58f61465-ef63-4031-a7b0-066b98a49768:2222',
                        'end': 2688,
                        'endNodeElementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:2688',
                        'identity': 2222,
                        'properties': {},
                        'start': 10463,
                        'startNodeElementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:10463',
                        'type': 'HAS_DATASET'},
    'source_node': {   'elementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:10463',
                       'identity': 10463,
                       'labels': ['DataCenter'],
                       'properties': {   'globalId': 'b28caa85-8229-5be8-8a07-723cef0c9847',
                                         'longName': 'Atmospheric Science Data Center, Science '
                                                     'Directorate, Langley Research Center, NASA',
                                         'shortName': 'NASA/LARC/SD/ASDC',
                                         'url': 'https://asdc.larc.nasa.gov/'}},
    'target_node': {   'elementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:2688',
                       'identity': 2688,
                       'labels': ['Dataset'],
                       'properties': {   'abstract': 'CAL_LID_L2_BlowingSnow_Antarctica-Standard-V1-00 '
                                                     'is the Cloud-Aerosol Lidar and Infrared '
                                                     'Pathfinder Satellite Observations (CALIPSO) '
                                                     'Lidar Level 2 Blowing Snow - Antarctica, '
                                                     'Version 1-00 data product. This product was '
                                                     'collected using the CALIPSO Cloud-Aerosol '
                                                     'Lidar with Orthogonal Polarization (CALIOP) '
                                                     'instrument and reports the distribution of '
                                                     'blowing snow properties based on '
                                                     'back-scatter retrievals over Antarctica. '
                                                     'Data collection for this product is '
                                                     'complete. CALIPSO was launched on April 28, '
                                                     '2006, to study the impact of clouds and '
                                                     "aerosols on the Earth's radiation budget and "
                                                     'climate. It flies in the international '
                                                     'A-Train constellation for coincident Earth '
                                                     'observations. The CALIPSO satellite '
                                                     'comprises three instruments: CALIOP, Imaging '
                                                     'Infrared Radiometer (IIR), and Wide Field '
                                                     'Camera (WFC). CALIPSO is a joint satellite '
                                                     'mission between NASA and the French Agency '
                                                     "CNES (Centre National D'Etudes Spatiales).",
                                         'cmrId': 'C1561247108-LARC_ASDC',
                                         'daac': 'NASA/LARC/SD/ASDC',
                                         'doi': '10.5067/CALIOP/CALIPSO/LID_L2_BLOWINGSNOW_ANTARCTICA-STANDARD-V1-00',
                                         'globalId': 'd9e9fc92-0efb-57df-b7b7-66aef4ca69cc',
                                         'landingPageUrl': 'https://www.doi.org/10.5067/CALIOP/CALIPSO/LID_L2_BLOWINGSNOW_ANTARCTICA-STANDARD-V1-00',
                                         'longName': 'CALIPSO Lidar Level 2 Blowing Snow - '
                                                     'Antarctica, V1-00',
                                         'pagerank_publication_dataset': 0.15000000000000002,
                                         'shortName': 'CAL_LID_L2_BlowingSnow_Antarctica-Standard-V1-00',
                                         'temporalExtentEnd': '2020-06-30T23:21:26.000Z',
                                         'temporalExtentStart': '2006-06-12T00:00:00.000000Z',
                                         'temporalFrequency': 'Unknown'}}}

MOCK_EDGE_OF_PROJECT = {   'relationship': {   'elementId': '5:58f61465-ef63-4031-a7b0-066b98a49768:1535',
                        'end': 6679,
                        'endNodeElementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:6679',
                        'identity': 1535,
                        'properties': {},
                        'start': 2266,
                        'startNodeElementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:2266',
                        'type': 'OF_PROJECT'},
    'source_node': {   'elementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:2266',
                       'identity': 2266,
                       'labels': ['Dataset'],
                       'properties': {   'abstract': 'CAL_LID_L2_05kmAPro-Standard-V4-21 is the '
                                                     'Cloud-Aerosol Lidar and Infrared Pathfinder '
                                                     'Satellite Observations (CALIPSO) Lidar Level '
                                                     '2 Aerosol Profile, Version 4-21 data '
                                                     'product. Data for this product was collected '
                                                     'using the CALIPSO Cloud-Aerosol Lidar with '
                                                     'Orthogonal Polarization (CALIOP) instrument. '
                                                     'The version of this product was changed from '
                                                     '4-20 to 4-21 to account for a change in the '
                                                     'operating system of the CALIPSO production '
                                                     'cluster. Data collection for this product is '
                                                     'ongoing.CALIPSO was launched on April 28, '
                                                     '2006, to study the impact of clouds and '
                                                     "aerosols on the Earth's radiation budget and "
                                                     'climate. It flies in the international '
                                                     'A-Train constellation for coincident Earth '
                                                     'observations. The CALIPSO satellite '
                                                     'comprises three instruments: CALIOP, Imaging '
                                                     'Infrared Radiometer (IIR), and Wide Field '
                                                     'Camera (WFC). CALIPSO is a joint satellite '
                                                     'mission between NASA and the French Agency '
                                                     "CNES (Centre National d'Etudes Spatiales).",
                                         'cmrId': 'C1978623316-LARC_ASDC',
                                         'daac': 'NASA/LARC/SD/ASDC',
                                         'doi': '10.5067/CALIOP/CALIPSO/CAL_LID_L2_05kmAPro-Standard-V4-21',
                                         'globalId': 'c657de3e-1d31-588d-83f6-bd4cca506b56',
                                         'landingPageUrl': 'https://www.doi.org/10.5067/CALIOP/CALIPSO/CAL_LID_L2_05kmAPro-Standard-V4-21',
                                         'longName': 'CALIPSO Lidar Level 2 Aerosol Profile, V4-21',
                                         'pagerank_publication_dataset': 0.9779335943087838,
                                         'shortName': 'CAL_LID_L2_05kmAPro-Standard-V4-21',
                                         'temporalExtentEnd': '2022-01-19T23:59:59.999999Z',
                                         'temporalExtentStart': '2020-07-01T00:00:00.000000Z',
                                         'temporalFrequency': 'Unknown'}},
    'target_node': {   'elementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:6679',
                       'identity': 6679,
                       'labels': ['Platform'],
                       'properties': {   'Type': 'Earth Observation Satellites',
                                         'globalId': '55dc21cf-5929-5024-9c61-9ee7af8146a4',
                                         'longName': 'Cloud-Aerosol Lidar and Infrared Pathfinder '
                                                     'Satellite Observations',
                                         'shortName': 'CALIPSO'}}}

MOCK_EDGE_HAS_PLATFORM = {   'relationship': {   'elementId': '5:58f61465-ef63-4031-a7b0-066b98a49768:6823',
                        'end': 6042,
                        'endNodeElementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:6042',
                        'identity': 6823,
                        'properties': {},
                        'start': 4225,
                        'startNodeElementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:4225',
                        'type': 'HAS_PLATFORM'},
    'source_node': {   'elementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:4225',
                       'identity': 4225,
                       'labels': ['Dataset'],
                       'properties': {   'abstract': 'NARSTO_EPA_SS_FRESNO_MET_DATA is North '
                                                     'American Research Strategy for Tropospheric '
                                                     'Ozone (NARSTO) Environmental Protection '
                                                     'Agency (EPA) Supersite (SS) Fresno, Beta '
                                                     'Attenuation Monitors (BAM) Meteorological '
                                                     'Data. This data set contains measurements '
                                                     'taken from six meteorological instruments '
                                                     'operated at the Fresno supersite from May '
                                                     '24, 2000 to December 31, 2006. The ambient '
                                                     'temperature was measured by a Met One '
                                                     'Instruments aspirated thermistor, Model '
                                                     '060A-2. The barometric pressure was measured '
                                                     'by a Met One pressure transducer, Model '
                                                     '090D. The relative humidity was measured by '
                                                     'a Met One aspirated thin film capacitor, '
                                                     'Model 083V. The solar radiation was measured '
                                                     'by a LI-COR Inc. pyranometer, Model '
                                                     'LI-200SA. The wind speed was measured by a '
                                                     'Met One Instruments 3-cup anemometer, Model '
                                                     '010-SC. The wind direction was measured by a '
                                                     'Met One Instruments High-Sensitivity wind '
                                                     'vane, Model 025-5C. All six instruments '
                                                     'reported 5 minute samples.The Fresno '
                                                     'Supersite is one of several Supersites '
                                                     'established in urban areas within the United '
                                                     'States by the EPA to better understand the '
                                                     'measurement, sources, and health effects of '
                                                     'suspended particulate matter (PM). The site '
                                                     'is located at 3425 First Street, '
                                                     'approximately 1 km north of the downtown '
                                                     'commercial district. First Street was a '
                                                     'four-lane artery with moderate traffic '
                                                     'levels. Commercial establishments, office '
                                                     'buildings, churches, and schools were '
                                                     'located north and south of the monitor. '
                                                     'Medium-density single-family homes and some '
                                                     'apartments were located in the blocks to the '
                                                     'east and west of First Street. The Fresno '
                                                     'Supersite began operation in May of 1999.The '
                                                     'EPA PM Supersites Program was an ambient air '
                                                     'monitoring research program designed to '
                                                     'provide information of value to the '
                                                     'atmospheric sciences, and human health and '
                                                     'exposure research communities. Eight '
                                                     'geographically diverse projects were chosen '
                                                     'to specifically address the following EPA '
                                                     'research priorities: (1) to characterize PM, '
                                                     'its constituents, precursors, co-pollutants, '
                                                     'atmospheric transport, and its source '
                                                     'categories that affect the PM in any region; '
                                                     '(2) to address the research questions and '
                                                     'scientific uncertainties about PM '
                                                     'source-receptor and exposure-health effects '
                                                     'relationships; and (3) to compare and '
                                                     'evaluate different methods of characterizing '
                                                     'PM including testing new and emerging '
                                                     'measurement methods.NARSTO, which has since '
                                                     'disbanded, was a public/private partnership, '
                                                     'whose membership spanned across government, '
                                                     'utilities, industry, and academe throughout '
                                                     'Mexico, the United States, and Canada. The '
                                                     'primary mission was to coordinate and '
                                                     'enhance policy-relevant scientific research '
                                                     'and assessment of tropospheric pollution '
                                                     'behavior; activities provide input for '
                                                     'science-based decision-making and '
                                                     'determination of workable, efficient, and '
                                                     'effective strategies for local and regional '
                                                     'air-pollution management. Data products from '
                                                     'local, regional, and international '
                                                     'monitoring and research programs are still '
                                                     'available.',
                                         'cmrId': 'C2241871216-LARC_ASDC',
                                         'daac': 'NASA/LARC/SD/ASDC',
                                         'doi': '10.5067/ASDCDAAC/NARSTO/0050',
                                         'globalId': '1e5365fa-bb19-5dd9-aedb-ed6ccd6fb22c',
                                         'longName': 'NARSTO EPA Supersite (SS) Fresno, Beta '
                                                     'Attenuation Monitors (BAM) Meteorological '
                                                     'Data',
                                         'pagerank_publication_dataset': 0.15000000000000002,
                                         'shortName': 'NARSTO_EPA_SS_FRESNO_MET_DATA',
                                         'temporalExtentEnd': '2007-01-01T23:59:59.999Z',
                                         'temporalExtentStart': '2000-05-24T00:00:00.000Z',
                                         'temporalFrequency': 'Unknown'}},
    'target_node': {   'elementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:6042',
                       'identity': 6042,
                       'labels': ['Platform'],
                       'properties': {   'Type': 'Permanent Land Sites',
                                         'globalId': 'afd5d636-a851-5584-9dc5-51e6adc5bdb3',
                                         'longName': 'FIXED OBSERVATION STATIONS',
                                         'shortName': 'FIXED OBSERVATION STATIONS'}}}

MOCK_EDGE_HAS_INSTRUMENT = {   'relationship': {   'elementId': '5:58f61465-ef63-4031-a7b0-066b98a49768:4711',
                        'end': 6960,
                        'endNodeElementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:6960',
                        'identity': 4711,
                        'properties': {},
                        'start': 6135,
                        'startNodeElementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:6135',
                        'type': 'HAS_INSTRUMENT'},
    'source_node': {   'elementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:6135',
                       'identity': 6135,
                       'labels': ['Platform'],
                       'properties': {   'Type': 'Permanent Land Sites',
                                         'globalId': 'afe4e2a5-1cf4-597e-aa00-0d28def7aa23',
                                         'longName': 'METEOROLOGICAL STATIONS',
                                         'shortName': 'METEOROLOGICAL STATIONS'}},
    'target_node': {   'elementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:6960',
                       'identity': 6960,
                       'labels': ['Instrument'],
                       'properties': {   'globalId': 'c6e7da4a-0788-5d76-853d-8b401181f369',
                                         'longName': 'PYRANOMETERS',
                                         'shortName': 'PYRANOMETERS'}}}

MOCK_EDGE_USES_DATASET = {   'relationship': {   'elementId': '5:58f61465-ef63-4031-a7b0-066b98a49768:6797',
                        'end': 2266,
                        'endNodeElementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:2266',
                        'identity': 6797,
                        'properties': {},
                        'start': 11699,
                        'startNodeElementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:11699',
                        'type': 'USES_DATASET'},
    'source_node': {   'elementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:11699',
                       'identity': 11699,
                       'labels': ['Publication'],
                       'properties': {   'abstract': 'Abstract. This study characterizes a massive '
                                                     'African dust intrusion into the Caribbean '
                                                     'Basin and southern US in June 2020, which is '
                                                     'nicknamed the Godzilla dust plume, using a '
                                                     'comprehensive set of satellite and '
                                                     'ground-based observations (including MODIS, '
                                                     'CALIOP, SEVIRI, AERONET, and EPA Air Quality '
                                                     'network) and the NASA GEOS global aerosol '
                                                     'transport model. The MODIS data record '
                                                     'registered this massive dust intrusion event '
                                                     'as the most intense episode over the past 2 '
                                                     'decades. During this event, the aerosol '
                                                     'optical depth (AOD) observed by AERONET and '
                                                     'MODIS peaked at 3.5 off the coast of West '
                                                     'Africa and 1.8 in the Caribbean Basin. '
                                                     'CALIOP observations show that the top of the '
                                                     'dust plume reached altitudes of 68 km in '
                                                     'West Africa and descended to about 4 km '
                                                     'altitude over the Caribbean Basin and 2 km '
                                                     'over the US Gulf of Mexico coast. The dust '
                                                     'intrusion event degraded the air quality in '
                                                     'Puerto Rico to a hazardous level, with '
                                                     'maximum daily PM10 concentration of 453 g m3 '
                                                     'recorded on 23 June. The dust intrusion into '
                                                     'the US raised the PM2.5 concentration on 27 '
                                                     'June to a level exceeding the EPA air '
                                                     'quality standard in about 40 % of the '
                                                     'stations in the southern US. Satellite '
                                                     'observations reveal that dust emissions from '
                                                     'convection-generated haboobs and other '
                                                     'sources in West Africa were large albeit not '
                                                     'extreme on a daily basis. However, the '
                                                     'anomalous strength and northern shift of the '
                                                     'North Atlantic Subtropical High (NASH) '
                                                     'together with the Azores low formed a closed '
                                                     'circulation pattern that allowed for '
                                                     'accumulation of the dust near the African '
                                                     'coast for about 4 d. When the NASH was '
                                                     'weakened and wandered back to the south, the '
                                                     'dust outflow region was dominated by a '
                                                     'strong African easterly jet that rapidly '
                                                     'transported the accumulated dust from the '
                                                     'coastal region toward the Caribbean Basin, '
                                                     'resulting in the record-breaking African '
                                                     'dust intrusion. In comparison to satellite '
                                                     'observations, the GEOS model reproduced the '
                                                     'MODIS observed tracks of the meandering dust '
                                                     'plume well as it was carried by the wind '
                                                     'systems. However, the model substantially '
                                                     'underestimated dust emissions from haboobs '
                                                     'and did not lift up enough dust to the '
                                                     'middle troposphere for ensuing long-range '
                                                     'transport. Consequently, the model largely '
                                                     'missed the satellite-observed elevated dust '
                                                     'plume along the cross-ocean track and '
                                                     'underestimated the dust intrusion into the '
                                                     'Caribbean Basin by a factor of more than 4. '
                                                     'Modeling improvements need to focus on '
                                                     'developing more realistic representations of '
                                                     'moist convection, haboobs, and the vertical '
                                                     'transport of dust.',
                                         'authors': '',
                                         'doi': '10.5194/ACP-21-12359-2021',
                                         'globalId': 'd0eaf90a-90e2-5fd5-a87b-1479936aa3d9',
                                         'pagerank_publication_dataset': 1.2262289231569214,
                                         'title': 'Observation and modeling of the historic '
                                                  'Godzilla African dust intrusion into the '
                                                  'Caribbean Basin and the southern US in June '
                                                  '2020',
                                         'year': '2021'}},
    'target_node': {   'elementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:2266',
                       'identity': 2266,
                       'labels': ['Dataset'],
                       'properties': {   'abstract': 'CAL_LID_L2_05kmAPro-Standard-V4-21 is the '
                                                     'Cloud-Aerosol Lidar and Infrared Pathfinder '
                                                     'Satellite Observations (CALIPSO) Lidar Level '
                                                     '2 Aerosol Profile, Version 4-21 data '
                                                     'product. Data for this product was collected '
                                                     'using the CALIPSO Cloud-Aerosol Lidar with '
                                                     'Orthogonal Polarization (CALIOP) instrument. '
                                                     'The version of this product was changed from '
                                                     '4-20 to 4-21 to account for a change in the '
                                                     'operating system of the CALIPSO production '
                                                     'cluster. Data collection for this product is '
                                                     'ongoing.CALIPSO was launched on April 28, '
                                                     '2006, to study the impact of clouds and '
                                                     "aerosols on the Earth's radiation budget and "
                                                     'climate. It flies in the international '
                                                     'A-Train constellation for coincident Earth '
                                                     'observations. The CALIPSO satellite '
                                                     'comprises three instruments: CALIOP, Imaging '
                                                     'Infrared Radiometer (IIR), and Wide Field '
                                                     'Camera (WFC). CALIPSO is a joint satellite '
                                                     'mission between NASA and the French Agency '
                                                     "CNES (Centre National d'Etudes Spatiales).",
                                         'cmrId': 'C1978623316-LARC_ASDC',
                                         'daac': 'NASA/LARC/SD/ASDC',
                                         'doi': '10.5067/CALIOP/CALIPSO/CAL_LID_L2_05kmAPro-Standard-V4-21',
                                         'globalId': 'c657de3e-1d31-588d-83f6-bd4cca506b56',
                                         'landingPageUrl': 'https://www.doi.org/10.5067/CALIOP/CALIPSO/CAL_LID_L2_05kmAPro-Standard-V4-21',
                                         'longName': 'CALIPSO Lidar Level 2 Aerosol Profile, V4-21',
                                         'pagerank_publication_dataset': 0.9779335943087838,
                                         'shortName': 'CAL_LID_L2_05kmAPro-Standard-V4-21',
                                         'temporalExtentEnd': '2022-01-19T23:59:59.999999Z',
                                         'temporalExtentStart': '2020-07-01T00:00:00.000000Z',
                                         'temporalFrequency': 'Unknown'}}}

MOCK_EDGE_HAS_SUBCATEGORY = {   'relationship': {   'elementId': '5:58f61465-ef63-4031-a7b0-066b98a49768:8656',
                        'end': 32060,
                        'endNodeElementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:32060',
                        'identity': 8656,
                        'properties': {},
                        'start': 31713,
                        'startNodeElementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:31713',
                        'type': 'HAS_SUBCATEGORY'},
    'source_node': {   'elementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:31713',
                       'identity': 31713,
                       'labels': ['ScienceKeyword'],
                       'properties': {   'globalId': '74b7fecb-61c7-584a-a4a9-0f549adfadea',
                                         'name': 'SEA SURFACE TOPOGRAPHY'}},
    'target_node': {   'elementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:32060',
                       'identity': 32060,
                       'labels': ['ScienceKeyword'],
                       'properties': {   'globalId': '03349af0-8956-5149-8900-6a29a6519cb0',
                                         'name': 'SEA SURFACE SLOPE'}}}

MOCK_EDGE_HAS_SCIENCEKEYWORD = {   'relationship': {   'elementId': '5:58f61465-ef63-4031-a7b0-066b98a49768:12332',
                        'end': 29443,
                        'endNodeElementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:29443',
                        'identity': 12332,
                        'properties': {},
                        'start': 3971,
                        'startNodeElementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:3971',
                        'type': 'HAS_SCIENCEKEYWORD'},
    'source_node': {   'elementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:3971',
                       'identity': 3971,
                       'labels': ['Dataset'],
                       'properties': {   'abstract': 'CLAMS_CV580_CAR data were collected during '
                                                     'the Chesapeake Lighthouse and Aircraft '
                                                     'Measurements for Satellites (CLAMS) '
                                                     'experiment.The Cloud Absorption Radiometer '
                                                     '(CAR) instrument is an airborne '
                                                     'multi-wavelength scanning radiometer, '
                                                     'designed to operate from a mounted position '
                                                     'aboard various aircraft, including the nose '
                                                     "cone of the University of Washington's "
                                                     'Convair CV-580. Developed by Dr. Michael '
                                                     'King at NASA Goddard Space Flight Center, '
                                                     'the CAR instrument measures radiance for 190 '
                                                     'degrees, and total view at 1 degree field of '
                                                     'view resolution. Instrument functions '
                                                     'include: * acquiring imagery of Earth '
                                                     'surface and Cloud features* single '
                                                     'scattering albedo determination of clouds* '
                                                     'angular distribution measuring of scattered '
                                                     'radiation* bidirectional reflectance '
                                                     'measuring of various surface types',
                                         'cmrId': 'C1567298630-LARC_ASDC',
                                         'daac': 'NASA/LARC/SD/ASDC',
                                         'doi': '10.5067/ASDC_DAAC/CLAMS/0002',
                                         'globalId': '128ef8f7-fc9c-576a-887a-7af0ad5fc2f0',
                                         'longName': 'CLAMS CV-580 Cloud Absorption Radiometer '
                                                     '(CAR)',
                                         'pagerank_publication_dataset': 0.15000000000000002,
                                         'shortName': 'CLAMS_CV580_CAR',
                                         'temporalExtentEnd': '2001-08-02T23:59:59.999Z',
                                         'temporalExtentStart': '2001-07-10T00:00:00.000Z',
                                         'temporalFrequency': 'Unknown'}},
    'target_node': {   'elementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:29443',
                       'identity': 29443,
                       'labels': ['ScienceKeyword'],
                       'properties': {   'globalId': '980fe165-5ff0-5b4b-bfb8-225e01cdd3a5',
                                         'name': 'CLOUDS'}}}

MOCK_EDGE_CITES = {   'relationship': {   'elementId': '5:58f61465-ef63-4031-a7b0-066b98a49768:66759',
                        'end': 16116,
                        'endNodeElementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:16116',
                        'identity': 66759,
                        'properties': {},
                        'start': 56272,
                        'startNodeElementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:56272',
                        'type': 'CITES'},
    'source_node': {   'elementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:56272',
                       'identity': 56272,
                       'labels': ['Publication'],
                       'properties': {   'DOI': '10.3390/rs15020297',
                                         'abstract': 'Floods are severe natural disasters that are '
                                                     'harmful and frequently occur across the '
                                                     'world. From May to July 2022, the strongest, '
                                                     'broadest, and longest rainfall event in '
                                                     'recent years occurred in Guangdong Province, '
                                                     'China. The flooding caused by continuous '
                                                     'precipitation and a typhoon resulted in '
                                                     'severe losses to local people and property. '
                                                     'During flood events, there is an urgent need '
                                                     'for timely and detailed flood inundation '
                                                     'mapping for areas that have been severely '
                                                     'affected. However, current satellite '
                                                     'missions cannot provide sufficient '
                                                     'information at a high enough spatio-temporal '
                                                     'resolution for flooding applications. In '
                                                     'contrast, spaceborne Global Navigation '
                                                     'Satellite System reflectometry technology '
                                                     "can be used to observe the Earth's surface "
                                                     'at a high spatio-temporal resolution without '
                                                     'being affected by clouds or surface '
                                                     'vegetation, providing a feasible scheme for '
                                                     'flood disaster research. In this study, '
                                                     'Cyclone Global Navigation Satellite System '
                                                     '(CYGNSS) L1 science data were processed to '
                                                     'obtain the change in the delay-Doppler map '
                                                     'and surface reflectivity (SR) during the '
                                                     'flood event. Then, a flood inundation map of '
                                                     'the extreme precipitation was drawn using '
                                                     'the threshold method based on the CYGNSS SR. '
                                                     'Additionally, the flooded areas that were '
                                                     'calculated based on the soil moisture from '
                                                     'the Soil Moisture Active Passive (SMAP) data '
                                                     'were used as a reference. Furthermore, the '
                                                     'daily Dry Wet Abrupt Alternation Index '
                                                     '(DWAAI) was used to identify the occurrence '
                                                     'of the flood events. The results showed good '
                                                     'agreement between the flood inundation that '
                                                     'was derived from the CYGNSS SR and SMAP soil '
                                                     'moisture. Moreover, compared with the SMAP '
                                                     'results, the CYGNSS SR can provide the daily '
                                                     'flood inundation with higher accuracy due to '
                                                     'its high spatio-temporal resolution. '
                                                     'Furthermore, the DWAAI can identify the '
                                                     'transformation from droughts to floods in a '
                                                     'relatively short period. Consequently, the '
                                                     'distributions of and variations in flood '
                                                     'inundation under extreme weather conditions '
                                                     'can be identified on a daily scale with good '
                                                     'accuracy using the CYGNSS data.',
                                         'authors': [   'Wei, Haohan',
                                                        'Yu, Tongning',
                                                        'Tu, Jinsheng',
                                                        'Ke, Fuyang'],
                                         'globalId': '33c36e1b-bdab-55c3-90e7-fd5767ff979e',
                                         'pagerank_publication_dataset': 0.15000000000000002,
                                         'title': 'Detection and Evaluation of Flood Inundation '
                                                  'Using CYGNSS Data during Extreme Precipitation '
                                                  'in 2022 in Guangdong Province, China',
                                         'year': '2023'}},
    'target_node': {   'elementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:16116',
                       'identity': 16116,
                       'labels': ['Publication'],
                       'properties': {   'abstract': 'Mapping inundation dynamics and flooding '
                                                     'extent is important for a wide variety of '
                                                     'applications, from providing disaster relief '
                                                     'and predicting infectious disease '
                                                     'transmission to quantifying the effects of '
                                                     "climate change on Earth's hydrologic cycle. "
                                                     'Due to the rapid and highly spatially '
                                                     'heterogeneous nature of flooding events, '
                                                     'acquiring data with both high spatial and '
                                                     'temporal resolutions is paramount, yet doing '
                                                     'so has remained a challenge in satellite '
                                                     'remote sensing. The potential for Global '
                                                     'Navigation Satellite System-Reflectometry '
                                                     '(GNSS-R) to help address this challenge has '
                                                     'been explored in several studies, the bulk '
                                                     'of which use data from the Cyclone GNSS '
                                                     '(CYGNSS) constellation of GNSS-R satellites. '
                                                     'This work presents a simple forward model '
                                                     'that describes how surface reflectivity '
                                                     'measured by CYGNSS should change due to '
                                                     'flooding for different land surface types. '
                                                     'We corroborate our model findings with '
                                                     'observations from the Amazon Basin and Lake '
                                                     'Eyre, Australia. Both the model and '
                                                     'observations indicate that the relationship '
                                                     'between surface reflectivity and surface '
                                                     'water extent strongly depends on the '
                                                     'micro-scale surface roughness of the land '
                                                     'and water. We show that the increase in '
                                                     'surface reflectivity due to flooding or '
                                                     'inundation is greatest in areas where the '
                                                     'surrounding land has dense vegetation. In '
                                                     'areas where the land surface surrounding '
                                                     'inundated areas is perfectly smooth, the '
                                                     'increase in surface reflectivity due to '
                                                     'flooding is not as strong, and confounding '
                                                     'effects of soil moisture and water roughness '
                                                     'could lead to large uncertainties in '
                                                     'resulting surface water retrievals. However, '
                                                     'even a few centimeters of surface roughness '
                                                     'will result in several dB sensitivity to '
                                                     'surface water, provided that the water is '
                                                     'smoother than the land surface itself.',
                                         'authors': '',
                                         'doi': '10.1016/J.RSE.2020.111869',
                                         'globalId': '02b9d40e-bfa4-5c84-a9bf-74646675ec06',
                                         'pagerank_publication_dataset': 1.062741033519781,
                                         'title': 'Estimating inundation extent using CYGNSS data: '
                                                  'A conceptual modeling study',
                                         'year': '2020'}}}

MOCK_EDGE_HAS_APPLIEDRESEARCHAREA = {   'relationship': {   'elementId': '5:58f61465-ef63-4031-a7b0-066b98a49768:309312',
                        'end': 29320,
                        'endNodeElementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:29320',
                        'identity': 309312,
                        'properties': {},
                        'start': 106805,
                        'startNodeElementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:106805',
                        'type': 'HAS_APPLIEDRESEARCHAREA'},
    'source_node': {   'elementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:106805',
                       'identity': 106805,
                       'labels': ['Publication'],
                       'properties': {   'DOI': '10.3390/universe8060297',
                                         'abstract': 'In the last decade, many experiments have '
                                                     'been planned, designed or constructed to '
                                                     'detect Ultra High Energy showers produced by '
                                                     'cosmic rays or neutrinos using the radio '
                                                     'technique. This technique consists in '
                                                     'detecting short radio pulses emitted by the '
                                                     'showers. When the detected wavelengths are '
                                                     'longer than typical shower length scales, '
                                                     'the pulses are coherent. Radio emission can '
                                                     'be simulated by adding up the contributions '
                                                     'of all the particle showers in a coherent '
                                                     'way. The first program to use this approach '
                                                     'was based on an algorithm developed more '
                                                     'than thirty years ago and referred to as '
                                                     '"ZHS". Since then, much progress has been '
                                                     'obtained using the ZHS algorithm with '
                                                     'different simulation programs to investigate '
                                                     'pulses from showers in dense homogeneous '
                                                     'media and the atmosphere, applying it to '
                                                     'different experimental initiatives, and '
                                                     'developing extensions to address different '
                                                     'emission mechanisms or special '
                                                     'circumstances. We here review this work, '
                                                     'primarily led by the authors in '
                                                     'collaboration with other scientists, '
                                                     'illustrating the connections between '
                                                     'different articles, and giving a pedagogical '
                                                     'approach to most of the work.',
                                         'authors': ['Alvarez-Muñiz, Jaime', 'Zas, Enrique'],
                                         'globalId': '11df227e-41d2-51b3-9b5c-66acc2a4ed68',
                                         'pagerank_publication_dataset': 0.15000000000000002,
                                         'title': 'Progress in the Simulation and Modelling of '
                                                  'Coherent Radio Pulses from Ultra High-Energy '
                                                  'Cosmic Particles',
                                         'year': '2022'}},
    'target_node': {   'elementId': '4:58f61465-ef63-4031-a7b0-066b98a49768:29320',
                       'identity': 29320,
                       'labels': ['ScienceKeyword'],
                       'properties': {   'globalId': 'ff5fbb4f-d201-5a90-a56c-f929a9f93b17',
                                         'name': 'AIR QUALITY'}}}
