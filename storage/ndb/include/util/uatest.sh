#!/usr/bin/env bash
set -euo pipefail
#echo "==local g++=="
#g++ -O3 -lstdc++ unescapedAscii.cpp && ./a.out
#g++ -O3 -lstdc++ -mavx2 unescapedAscii.cpp && ./a.out
#echo "==local clang=="
#clang -O3 -lstdc++ unescapedAscii.cpp && ./a.out
#clang -O3 -lstdc++ -mavx2 unescapedAscii.cpp && ./a.out
echo "==dev5=="
scp unescapedAscii.cpp rondb_simd.h dev5.devnet.hops.works:ua/
ssh dev5.devnet.hops.works "cd ua && g++ -O3 -lstdc++ unescapedAscii.cpp && ./a.out && g++ -O3 -lstdc++ -mavx2 unescapedAscii.cpp && ./a.out && g++ -O3 -lstdc++ -mavx512f -mavx512bw unescapedAscii.cpp && ./a.out"
#echo "==thearmchair=="
#scp unescapedAscii.cpp rondb_simd.h thearmchair.devnet.hops.works:ua/
#ssh thearmchair.devnet.hops.works "cd ua && g++ -O3 -lstdc++ unescapedAscii.cpp && ./a.out"
