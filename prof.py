import pstats

p = pstats.Stats('t.prof')

p.sort_stats('calls').print_stats()
