#!/usr/bin/env bash

timeincr=2


ls * | python /g/data1/u46/users/dra547/yeartosrt.py -timeincr $timeincr > subs.srt
mkdir tmp
cp *.png tmp
filename=`ls *.png | head -1`
dims=`identify $filename | cut -d' ' -f3`
newdims=`python -c "x, y = map(int, '$dims'.split('x')); scale = y / 1080; y = 1080; x = int(x/scale); x = x + 1 if x %2 == 1 else x; print('%sx%s' % (x,y))"`
mogrify -geometry $newdims\! tmp/*.png
~/ffmpeg-3.1.2-64bit-static/ffmpeg -framerate 1/$timeincr -pattern_type glob -i tmp/\*.png -c:v libx264 -pix_fmt yuv420p -r 30  -vf subtitles=subs.srt:force_style='FontName=DejaVu Sans' $1_video.mp4
