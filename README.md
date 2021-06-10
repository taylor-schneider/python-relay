# Relays

The relay is an abstraction layer for a distributed processing framework. 
In a nutshell it allows for event based functions and transformations to be executed triggered by message bus events.
Currently the Active MQ message bus is supported, but adding additional message busses should be possible.
The Relay can maintain state (in a culumnar pandas dataframe).

While this documentation is lacking, the unit tests provide some insights into how the objects can be used.

## Use Cases
- Processing real time data
- Replaying data cached on a bus or in a database

## Installation

To install the library:
- open a cmd shell
- cd to the root of this project (where theREADME is located)
- run the following command to install the project in developer mode:
` pip install -e .`
