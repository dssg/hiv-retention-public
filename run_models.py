import sqlalchemy
import yaml
import logging
import os
import argparse

from triage.experiments import SingleThreadedExperiment
#from triage.component.timechop.plotting import visualize_chops
from triage import create_engine

def parse_args():
    parser = argparse.ArgumentParser(
        description='This is DSSG JOCO 2.0\n')

    parser.add_argument('-c', '--config',
                        action='store',
                        dest='config_path',
                        required=True,
                        help='Absolute filepath for a yaml file containing the experiment configurations.')

    parser.add_argument('-p', '--path-to-models',
                        action='store',
                        dest='project_path',
                        required=True,
                        help="Absolute path to directory to store models and matrices.")

    return parser.parse_args()

def main():
    args = parse_args()

    dburl = os.environ['DBURL']
    hiv_engine = create_engine(dburl, pool_pre_ping=True)

    with open(args.config_path) as f:
        experiment_config = yaml.load(f)
    experiment = SingleThreadedExperiment(
        config=experiment_config,
        db_engine=hiv_engine,
        project_path=args.project_path,
        replace=False
    )

    experiment.validate()
    experiment.run()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s %(levelname)s] %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")

    main()
