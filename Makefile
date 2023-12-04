CXX = g++
CFLAGS = -std=c++17 -O3 -march=native -L./  
# CFLAGS = -std=c++17 -O0 -g -march=native -L./  

CURR := $(shell pwd)
CCEH_F := $(CFLAGS) -DCCEHT -std=c++17 -O3 -march=native \
	-L./third/pmdk/src/nondebug \
	-I./third/pmdk/src/include  -Wl,-rpath,$(CURR)/third/pmdk/src/nondebug

DASH_F := $(CFLAGS) -DDASHT -std=c++17 -O3 -march=native \
	-L./third/pmdk/src/nondebug \
	-I./third/pmdk/src/include  -Wl,-rpath,$(CURR)/third/pmdk/src/nondebug

PCM := -I./pcm -L./pcm

CFLAGS_PMDK := -std=c++17 -O3 -I./ -L./

tar = HESH HALO VIPER SOFT DASH CCEH  

all: $(tar)

$(tar): LIBPCM

LIBPCM:
	make -C pcm

libHesh.a: Hesh/*.h
	$(CXX) $(CFLAGS) -c -o libHesh.o $<
	ar rv libHesh.a libHesh.o

libHalo.a: third/Halo/Halo.cpp third/Halo/Halo.hpp third/Halo/Pair_t.h
	$(CXX) $(CFLAGS) -c -o libHalo.o $<
	ar rv libHalo.a libHalo.o

libSOFT.a: third/SOFT/ssmem.cpp  third/SOFT/*
	$(CXX) $(CFLAGS) -c -o libSOFT.o $<
	ar rv libSOFT.a libSOFT.o


HESH: benchmark.cpp libHesh.a hash_api.h
	$(CXX) -DHESHT $(CFLAGS) $(PCM) -o $@ $< -pthread -mavx -lPCM -lpmem 

HALO: benchmark.cpp libHalo.a hash_api.h 
	$(CXX) -DHALOT $(CFLAGS) $(PCM) -o $@ $< -lHalo -pthread -mavx -lPCM -lpmem

VIPER: benchmark.cpp hash_api.h third/viper/*
	$(CXX) -DVIPERT $(CFLAGS) $(PCM) -o $@ $< -pthread -lpmem -lPCM 

SOFT: benchmark.cpp libSOFT.a hash_api.h
	$(CXX) -DSOFTT $(CFLAGS) $(PCM) -o $@ $< -lSOFT -pthread -lvmem -lPCM 


CUSTOM_PMDK:
	chmod +x ./third/pmdk/utils/check-os.sh 
	make -C ./third/pmdk/src

DASH: benchmark.cpp hash_api.h CUSTOM_PMDK
	$(CXX) $(DASH_F) $(PCM) -o $@ $< -lpthread -lpmemobj -lPCM 

CCEH: benchmark.cpp hash_api.h CUSTOM_PMDK
	$(CXX) $(CCEH_F) $(PCM) -o $@ $< -lpthread -lPCM -lpmemobj



clean:
	rm -f *.o *.a $(tar)
cleanAll:
	rm -f *.o *.a $(tar)
	make -C pcm clean
	make -C ./third/pmdk/src clean