Installation
============

HyperKnowledge Event Sourcing has been installed on Ubuntu and MacOS. Familiarity with most common components and installation procedures is assumed.

Prerequisites: Ubuntu Jammy
---------------------------

.. code-block:: shell-session

    sudo apt install python3.10-dev postgresql-server-dev-15

Prerequisites: Mac
------------------

.. code-block:: shell-session

    brew install python@3.10 postgresql@14


Notes on prerequisites
----------------------

* Python 3.10 is assumed, as found on Ubuntu Jammy. Python 3.11 also works.
* Postgres 15 is used on Ubuntu, but on mac, the homebrew postgres 15 recipe does not handle extensions as well as 14.


Postgres extensions
-------------------

Install the following two postgres extensions:

* https://github.com/fboulnois/pg_uuidv7.git
* https://github.com/michelp/pgjwt.git


HyperKnowledge
--------------

1. Clone the repository and ``cd`` into it
2. Create a virtual environment (``python3.10 -mvenv venv``) and activate it (``. ./venv/bin/activate``)
3. Install the application (``pip install -e .``)
  1. Note that the PyMiniRacer dependency can be hard to install. There is a mac wheel [here](https://idealoom.org/wheelhouse/py_mini_racer-0.6.0-py2.py3-none-macosx_13_0_arm64.whl)
4. Create a skeleton config.ini file by calling initial setup. Exact arguments will depend on platform. The point is to pass database administrator credentials.

  1. Ubuntu, assuming a postgres user exists, and the current user is a sudoer:

    1. ``python scripts/initial_setup.py --app_name HyperKnowledge --sudo -u postgres``
    2. Note: I have a non-sudoer user to run HyperKnowledge, but login as a sudoer user when necessary for some commands.

  2. Mac, assuming the database accepts the logged-in user as a database admin:

    1. ``python scripts/initial_setup.py --app_name HyperKnowledge``

  3. Note that calls to ``initial_setup.py`` can be repeated without losing information. More options are given in the ``--help``

5. Initialize the development database

  1. ``python scripts/db_updater.py init``
  2. ``python scripts/db_updater.py deploy``
  3. The last command can and should be reapplied to run migrations whenever changes are made to the database schema.
  4. The need to do so can be verified with ``python scripts/db_updater.py status``.
  5. Note: The initial deployment may require a sudoer user on ubuntu.

Then you should be able to run the server for development with

`uvicorn hyperknowledge.eventdb.server:app --reload`

Or run the test suite with `pytest test`
