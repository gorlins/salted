from datetime import date
from hashlib import sha256

from luigi import DateIntervalParameter, DateParameter, LocalTarget, Task, build
from luigi.date_interval import Week
from luigi.task import flatten
import pandas as pd


def get_salted_version(task):
    """Create a salted id/version for this task and lineage

    :returns: a unique, deterministic hexdigest for this task
    :rtype: str
    """

    msg = ""

    # Salt with lineage
    for req in flatten(task.requires()):
        # Note that order is important and impacts the hash - if task
        # requirements are a dict, then consider doing this is sorted order
        msg += get_salted_version(req)

    # Uniquely specify this task
    msg += ','.join([

            # Basic capture of input type
            task.__class__.__name__,

            # Change __version__ at class level when everything needs rerunning!
            task.__version__,

        ] + [
            # Depending on strictness - skipping params is acceptable if
            # output already is partitioned by their params; including every
            # param may make hash *too* sensitive
            '{}={}'.format(param, value)
            for param, value in task.param_kwargs.items()
        ]
    )
    return sha256(msg.encode()).hexdigest()


class Streams(Task):
    date = DateParameter()
    __version__ = '1.0'

    def run(self):
        # This really should be an external task, but for simplicity we'll make
        # fake data

        with self.output().open('w') as out_file:
            df = pd.DataFrame(
                {'artist':['Scott', 'Sally'],
                 'track':['Python on my mind', 'What I like about R']})

            df.to_csv(out_file, sep='\t')

    def output(self):
        return LocalTarget('data/stream/{}.tsv'.format(self.date))


class AggregateArtists(Task):

    date_interval = DateIntervalParameter()

    __version__ = '1.2 - bugfix'

    def output(self):
        return LocalTarget(
            "data/artist_streams_{}-{}.tsv".format(
                self.date_interval,
                get_salted_version(self)[:8]
            ))

    def requires(self):
        return [Streams(date=date) for date in self.date_interval]

    def run(self):
        dfs = []

        for input in self.input():
            with input.open('r') as in_file:
                df = pd.read_csv(in_file, sep='\t')
                dfs.append(df.groupby('artist').size().to_frame('count'))

        together = pd.concat(dfs).reset_index().groupby('artist').sum()
        with self.output().open('w') as out_file:
            together.to_csv(out_file, sep='\t')


if __name__ == '__main__':
    agg = AggregateArtists(date_interval=Week.from_date(date(2018, 3, 7)))

    build([agg], local_scheduler=True)