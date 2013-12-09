import numpy as np
import operator
from mrjob.job import MRJob
from itertools import combinations, permutations
from scipy.stats.stats import pearsonr
import os
import sys
import time
import glob
import datetime
import sqlite3
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from pylab import *
ext='.h5'

class BuildFrame(MRJob):

    def steps(self):
        print "I'm doing the steps"
        "the steps in the map-reduce process"
        thesteps = [
          #  self.mr(mapper=self.line_mapper, reducer=self.users_items_collector),
            self.mr(mapper=self.pair_items_mapper, reducer=self.calc_sim_collector)
        ]
        return thesteps

    def pair_items_mapper(self, user_id, values):
        print "I'm inside the mapper"
        basepath = '/Users/anastasiyaborys/Desktop/cs109/Final_Project'
        msd_subset_path = basepath + '/Music_Magic/MillionSongSubset'
        msd_subset_data_path = os.path.join(msd_subset_path, 'data')
        basedir = msd_subset_data_path
        for root, dirs, files in os.walk(basedir):
            print "inside the roots loop"
            files = glob.glob(os.path.join(root,'*'+ext))
            #print "These are the files " + str(files)
            yield (1,files)


    def calc_sim_collector(self, files):
        print "went into the reducer"
        for f in files :
            
            h5 = GETTERS.open_h5_file_read(f)
            
            # song information
            song_id = GETTERS.get_song_id(h5) # order the information by song
            song_title = GETTERS.get_title(h5)
            song_hotness = GETTERS.get_song_hotttnesss(h5)
            song_dancability = GETTERS.get_danceability(h5)
            song_duration = GETTERS.get_duration(h5)
            song_loudness = GETTERS.get_loudness(h5)
            song_year = GETTERS.get_year(h5)
            analysis_sample_rate = GETTERS.get_analysis_sample_rate(h5)
            energy = GETTERS.get_energy(h5)
            
            #extra
            bars_confidence = GETTERS.get_bars_confidence(h5)
            beats_confidence = GETTERS.get_beats_confidence(h5)
            key = GETTERS.get_key(h5)
            key_confidence = GETTERS.get_key_confidence(h5)
            mode = GETTERS.get_mode(h5)
            mode_confidence = GETTERS.get_mode_confidence(h5)
            tempo = GETTERS.get_tempo(h5)
            
            # artist information
            album_name = GETTERS.get_release(h5)
            artist_id = GETTERS.get_artist_id(h5)
            artist_name = GETTERS.get_artist_name(h5)
            artist_location = GETTERS.get_artist_location(h5)
            artist_hotness = GETTERS.get_artist_hotttnesss(h5)
            similar_artists = GETTERS.get_similar_artists(h5)
            artist_tags = GETTERS.get_artist_mbtags(h5)
            
            #extra
            artist_familiarity = GETTERS.get_artist_familiarity(h5)
            artist_terms = GETTERS.get_artist_terms(h5)
            
            row = [dict(Song_ID = song_id, Artist_ID = artist_id, Title=song_title,
                                           Song_Hotness=song_hotness, Danceability=song_dancability,
                                           Duration=song_duration, Loudness=song_loudness, Year=song_year,
                                           Album_Name = album_name, Bars_Confidence = bars_confidence,
                                           #Beats_confidence = beats_confidence,
                                           #Key_confidence = key_confidence,
                                           Mode = mode, Key = key,
                                           Mode_confidence = mode_confidence, Tempo = tempo,
                                           #Artist_familiarity = artist_familiarity, Artist_terms = artist_terms,
                                           Analysis_Sample_Rate = analysis_sample_rate, Song_Energy = energy,
                                           Artist_Name=artist_name, Artist_location=artist_location,
                                           Artist_Hotness=artist_hotness, Similar_Artists=similar_artists,
                                           Artist_Tags=artist_tags)]
            """data_df = data_df.append([dict(Song_ID = song_id, Artist_ID = artist_id, Title=song_title,
                                           Song_Hotness=song_hotness, Danceability=song_dancability,
                                           Duration=song_duration, Loudness=song_loudness, Year=song_year,
                                           Album_Name = album_name, Bars_Confidence = bars_confidence,
                                           #Beats_confidence = beats_confidence,
                                           #Key_confidence = key_confidence,
                                           Mode = mode, Key = key,
                                           Mode_confidence = mode_confidence, Tempo = tempo,
                                           #Artist_familiarity = artist_familiarity, Artist_terms = artist_terms,
                                           Analysis_Sample_Rate = analysis_sample_rate, Song_Energy = energy,
                                           Artist_Name=artist_name, Artist_location=artist_location,
                                           Artist_Hotness=artist_hotness, Similar_Artists=similar_artists,
                                           Artist_Tags=artist_tags)], ignore_index=True)"""
            print row
            h5.close()
        
        yield row


#Below MUST be there for things to work
if __name__ == '__main__':
    BuildFrame.run()
