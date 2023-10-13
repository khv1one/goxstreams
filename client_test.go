package goxstreams

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-redis/redismock/v9"
	"github.com/redis/go-redis/v9"
)

const (
	stream   = "mystream"
	group    = "mygroup"
	consumer = "consumer"
)

func suite(batch int64) (streamClient, redismock.ClientMock) {
	client, mock := redismock.NewClientMock()
	streamClient := newClient(client, clientParams{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		Batch:    batch,
		NoAck:    false,
	})

	return streamClient, mock
}

func makeMockStreams(count int64) []redis.XStream {
	mockMessage := redis.XMessage{ID: "123", Values: map[string]interface{}{"body": "{\"Message\":\"message\",\"Name\":\"name\",\"Foo\":691,\"Bar\":500,\"SubEvent\":{\"BarBar\":\"1234\",\"FooFoo\":{\"FooFooFoo\":777}"}}
	mockMessages := make([]redis.XMessage, count)
	for i := 0; int64(i) < count; i++ {
		mockMessages[i] = mockMessage
	}

	mockStream := redis.XStream{Stream: stream, Messages: mockMessages}

	return []redis.XStream{mockStream}
}

func benchmarkReadEvents(b *testing.B, size int64) {
	client, mock := suite(size)
	mockMessages := makeMockStreams(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		mock.ExpectXReadGroup(client.groupReadArgs).SetVal(mockMessages)
		b.StartTimer()
		buf, _ := client.readEvents(context.Background())
		client.eventPool.xMessagePut(buf)
	}
	b.ReportAllocs()
}

func BenchmarkReadEvents10(b *testing.B) {
	benchmarkReadEvents(b, 10)
}

func BenchmarkReadEvents100(b *testing.B) {
	benchmarkReadEvents(b, 100)
}

func BenchmarkReadEvents1000(b *testing.B) {
	benchmarkReadEvents(b, 1000)
}

func BenchmarkReadEvents10000(b *testing.B) {
	benchmarkReadEvents(b, 10000)
}

func benchmarkXStreamToXRawMessage(b *testing.B, size int64) {
	client, _ := suite(size)
	mockMessages := makeMockStreams(size)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := client.xStreamToXRawMessage(mockMessages)
		client.eventPool.xMessagePut(buf)
	}
	b.ReportAllocs()
}

func BenchmarkXStreamToXRawMessage10(b *testing.B) {
	benchmarkXStreamToXRawMessage(b, 10)
}

func BenchmarkXStreamToXRawMessage100(b *testing.B) {
	benchmarkXStreamToXRawMessage(b, 100)
}

func BenchmarkXStreamToXRawMessage1000(b *testing.B) {
	benchmarkXStreamToXRawMessage(b, 1000)
}

func BenchmarkXStreamToXRawMessage10000(b *testing.B) {
	benchmarkXStreamToXRawMessage(b, 10000)
}

func makeMockClaimMessages(count int64) []redis.XMessage {
	mockMessage := redis.XMessage{ID: "123", Values: map[string]interface{}{"body": "{\"Message\":\"message\",\"Name\":\"name\",\"Foo\":691,\"Bar\":500,\"SubEvent\":{\"BarBar\":\"1234\",\"FooFoo\":{\"FooFooFoo\":777}"}}
	mockMessages := make([]redis.XMessage, count)
	for i := 0; int64(i) < count; i++ {
		mockMessage.ID = fmt.Sprintf("%s%d", mockMessage.ID, i)
		mockMessages[i] = mockMessage
	}

	return mockMessages
}

func makeMockPendingMessages(count int64) []redis.XPendingExt {
	mockPending := redis.XPendingExt{ID: "123", RetryCount: 0}
	mockPendings := make([]redis.XPendingExt, count)
	for i := 0; int64(i) < count; i++ {
		mockPending.ID = fmt.Sprintf("%s%d", mockPending.ID, i)
		mockPendings[i] = mockPending
	}

	return mockPendings
}

func benchmarkPreparePendings(b *testing.B, size int64) {
	client, _ := suite(size)
	mockPendings := makeMockPendingMessages(size)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pendingsIdsPtr, pendingCountByIDPtr := client.preparePendings(mockPendings)
		client.eventPool.stringPut(pendingsIdsPtr)
		client.eventPool.stringIntMapPut(pendingCountByIDPtr)
	}
	b.ReportAllocs()
}

func BenchmarkPreparePendings10(b *testing.B) {
	benchmarkPreparePendings(b, 10)
}

func BenchmarkPreparePendings100(b *testing.B) {
	benchmarkPreparePendings(b, 100)
}

func BenchmarkPreparePendings1000(b *testing.B) {
	benchmarkPreparePendings(b, 1000)
}

func BenchmarkPreparePendings10000(b *testing.B) {
	benchmarkPreparePendings(b, 10000)
}

func benchmarkReadFailEvents(b *testing.B, size int64) {
	client, mock := suite(size)
	mockMessages := makeMockClaimMessages(size)
	mockPendings := makeMockPendingMessages(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		mock.ExpectXPendingExt(client.pendingArgs).SetVal(mockPendings)
		claimIds := make([]string, 0, len(mockPendings))
		for _, p := range mockPendings {
			claimIds = append(claimIds, p.ID)
		}
		client.claimArgs.Messages = claimIds
		mock.ExpectXClaim(client.claimArgs).SetVal(mockMessages)
		b.StartTimer()
		buf, _ := client.readFailEvents(context.Background())
		client.eventPool.xMessagePut(buf)
	}
	b.ReportAllocs()
}

func BenchmarkReadFailEvents10(b *testing.B) {
	benchmarkReadFailEvents(b, 10)
}

func BenchmarkReadFailEvents100(b *testing.B) {
	benchmarkReadFailEvents(b, 100)
}

func BenchmarkReadFailEvents1000(b *testing.B) {
	benchmarkReadFailEvents(b, 1000)
}

func BenchmarkReadFailEvents10000(b *testing.B) {
	benchmarkReadFailEvents(b, 10000)
}
