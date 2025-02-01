filename = 'creates_data_filtered'
efilename = 'exercises_data_filtered'

n = 5000

array A[n]

samples(x) = $0 > (n-1) ? n : int($0+1)
mod(x) = int(x) % n
avg_n(x) = (A[mod($0)+1]=x, (sum [i=1:samples($0)] A[i]) / samples($0))

set term pngcairo size 3840, 2160
set output "events_per_s.png"

set title 'Apollo Ledger Events Over Time'
set xdata time
set timefmt '%Y-%m-%dT%H:%M:%S'
set autoscale
set datafile sep ','
set format x '%H:%M:%.3S'
set ylabel 'Event Count'
set xlabel 'Time'
set yrange [0:200]
set samples 1000
plot filename.".log" using 2:1 w l lw 1 lc rgb "blue" title "Creates", \
     efilename.".log" using 2:1 w l lc rgb "red" lw 1 title "Exercises", \
     filename.".log" using 2:(avg_n($1)) w l lc rgb "pink" lw 1 title "Creates Average", \
     efilename.".log" using 2:(avg_n($1)) w l lc rgb "green" lw 1 title "Exercises Average"
