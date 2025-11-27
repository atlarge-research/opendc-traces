# Carbon Traces

Carbon Traces define the carbon intensity of the energy in a region during a period of time.

Carbon Traces are defined using parquet. This folder provides all tools to create new Carbon Traces. [carbon_example.py] shows how to create a simple Carbon Trace from a Pandas DataFrame. 

The traces folder contains 158 Carbon Traces collected from the Electricity Maps service (https://portal.electricitymaps.com/datasets). 

When using these traces it is important to cite Electricity Maps as follows: 

>Electricity Maps (2025). {COUNTRY NAME} {YEAR} {INTERVAL} Carbon Intensity Data (Version January 27, 2025). Electricity Maps. https://www.electricitymaps.com


For more information about how OpenDC uses Carbon Traces see [this](https://atlarge-research.github.io/opendc/docs/documentation/Input/Topology/PowerSource).