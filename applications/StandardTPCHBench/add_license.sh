for f in */*/*.h; do
  cat copyright.txt $f > $f.new
  mv $f.new $f
done
