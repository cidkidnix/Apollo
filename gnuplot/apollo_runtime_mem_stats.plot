filename = 'alloc'

n = 5000

array A[n]

samples(x) = $0 > (n-1) ? n : int($0+1)
mod(x) = int(x) % n
avg_n(x) = (A[mod($0)+1]=x, (sum [i=1:samples($0)] A[i]) / samples($0))

set term x11
# set output "memstats.png"

set title 'Apollo GC Alloc Size'
set autoscale y
set autoscale x
set datafile sep ','
set xdata time
set timefmt '%Y-%m-%dT%H:%M:%S'
set format x '%H:%M:%.2S'
set ylabel 'Allocated Memory (MB)'
set xlabel 'Time'
set samples 1000
plot filename.".log" using 2:(($1/1024/1024)) w l lw 1 title "Allocated Memory", \
     filename.".log" using 2:(avg_n($1/1024/1024)) w l lc rgb "red" lw 3 title "Average Allocated Memory"
while (1) {
  pause 1
  replot
}
