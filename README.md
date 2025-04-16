# mardiportal-workflowtools
A collection of utility functions and helpers used when building workflows for the MaRDI Portal — for example, tools to update or interact with the MaRDI Knowledge Graph.


## Installation

Clone the repository and install the package locally:

```bash
git clone https://github.com/MaRDI4NFDI/mardiportal-workflowtools.git
cd mardiportal-workflowtools
python -m pip install --upgrade pip setuptools
python -m pip install .
```

## Secrets Configuration
Some tools require credentials (e.g., for authenticated API access). You can provide them in a simple key-value file named secrets.conf:
```ini
mardi-kg-user=your-username
mardi-kg-password=your-password
```
Make sure this file is kept outside version control (add it to .gitignore).

## Example Usage
```pyhon
from mardiportal.workflowtools import some_utility_function
```
