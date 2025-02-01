filename = 'batches'
# sfilename = 'batch_size_500'
# ssfilename = 'batch_size_300'
n = 5000

array A[n]

samples(x) = $0 > (n-1) ? n : int($0+1)
mod(x) = int(x) % n
avg_n(x) = (A[mod($0)+1]=x, (sum [i=1:samples($0)] A[i]) / samples($0))

set term x11

set title 'Apollo Write Latency'
set autoscale y
set autoscale x
set datafile sep ','
set ylabel 'ms'
set xlabel 'Time'
set xdata time
set timefmt '%Y-%m-%dT%H:%M:%S'
set format x '%H:%M:%.2S'
set samples 1000
plot filename.".log" using 2:1 w l lw 1 title "Batch Size", \
     filename.".log" using 2:(avg_n($1)) w l lc rgb "red" lw 3 title "Average Batch Size"

while (1) {
  pause 1
  replot
}
#     sfilename.".log" using 1 w l lc rgb "blue" lw 1 title "Batch Size 500", \
#     sfilename.".log" using (avg_n($1)) w l lc rgb "green" lw 3 title "Average (Batch Size 500)", \
#     ssfilename.".log" using 1 w l lc rgb "black" lw 1 title "Batch Size 300", \
#     ssfilename.".log" using (avg_n($1)) w l lc rgb "pink" lw 3 title "Average (Batch Size 300)"
