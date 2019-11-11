package main

import (
	"context"
	"github.com/vearch/vearch/proto/pspb"
	"google.golang.org/grpc"
	"log"
)

/**
this case is use grpc to search by router
*/
func main() {
	conn, err := grpc.Dial("127.0.0.1:9002", grpc.WithInsecure())

	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	defer func() {
		if err := conn.Close(); err != nil {
			panic(err)
		}
	}()

	api := pspb.NewRpcApiClient(conn)

	//test no query
	response, err := api.Search(context.Background(), &pspb.RpcSearchRequest{
		Head:      &pspb.RpcRequestHead{TimeOutMs: 10000},
		DbName:    "test_vector_db",
		SpaceName: "vector_space",
	})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Println(response)


	//test with query
	response, err = api.Search(context.Background(), &pspb.RpcSearchRequest{
		Head:      &pspb.RpcRequestHead{TimeOutMs: 10000},
		DbName:    "test_vector_db",
		SpaceName: "vector_space",
		Query:`
			{
				"query": {
					"and": [
						{
							"field": "vector",
							"feature": [0.88658684,0.9873159,0.68632215,-0.114685304,-0.45059848,0.5360963,0.9243208,0.14288005,0.9383601,0.17486687,0.3889527,0.91680753,0.6597193,0.52906346,0.5491872,-0.24706548,0.28541148,0.87731135,-0.18872026,0.28016,0.14826365,0.7217548,0.66360927,0.839685,0.29014188,-0.7303055,0.31786093,0.7611028,0.38408384,0.004707908,0.27696127,0.6069607,0.52147454,0.34435293,0.5665409,0.9676775,0.9415799,-0.95000356,-0.7441306,0.32473814,0.24417956,0.4114195,-0.15658693,0.9567978,0.91448873,0.8040493,0.7370252,0.41042542,-0.12714817,0.7344759,0.95486677,0.6752892,0.79088193,0.27843192,0.7594493,0.96637094,0.21354128,0.14667709,0.52713686,0.39803344,0.13063455,-0.26041254,0.21177465,0.0889158,0.7040157,0.9184541,0.33231667,0.109015055,0.7252709,0.85923946,0.6874303,0.9188243,0.44670975,0.6534332,0.67833525,0.40294313,0.76628596,0.722926,0.2507119,0.86939317,0.1049489,0.5707651,0.89342695,0.89022624,0.06606513,0.46363428,0.8836891,0.8416466,0.43164334,-0.059498303,0.25076458,0.91614866,0.21405962,0.07442343,0.8398273,-0.518248,0.4477598,0.54731685,0.39200985,0.2999862,0.22204888,0.9051194,0.7241311,0.9049213,0.48899868,0.11941989,0.45151904,0.9315986,0.17897557,0.759705,0.2549287,0.96008617,0.25688004,0.5925487,0.3069243,0.9171891,0.46981755,0.14557107,0.8900092,0.84537476,0.5608369,0.6909559,0.777092,0.66562796,0.6040272,0.77930593,0.59144366,0.12506102],
							"format": "normal"
						}
					],
					"direct_search_type": 0,
					"online_log_level": "debug"
				},
				"size": 10,
				"sort": [
					{
						"_score": {
							"order": "asc"
						}
					}
				],
				"quick": true,
				"vector_value": true
			}
		`,
	})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Println(response)
}
