# -fsanitize=undefined
CXXFLAGS += -std=c++11 --save-temps -O3 -Wall -Wextra -pedantic -pthread
#-march=skylake-avx512
LDFLAGS += -pthread -lrbd -g -lrados -lstdc++

#CC=clang-6.0

all: test main

test: qed_main.o qed.o
	$(CC) $^ -o $@ $(LDFLAGS)

main: main.o qed.o
	$(CC) $^ -o $@ $(LDFLAGS)

builddep:
	sudo apt install -y --no-install-recommends librbd-dev


.cpp.o:
	$(CC) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

indent: main.cpp qed.cpp qed.h qed_main.cpp limitedq.h
	clang-format-6.0 -style='{ColumnLimit: 120}' -i $^

clean:
	rm -f *.[oais] *.bc *.ii main a.out


