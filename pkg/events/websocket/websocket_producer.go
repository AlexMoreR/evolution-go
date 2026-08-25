package websocket_producer

import (
	"net/http"
	"strings"
	"sync"

	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	"github.com/gomessguii/logger"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		logger.LogInfo("Verificando origem da conexão WebSocket")
		return true
	},
}

type websocketProducer struct {
	clients       map[string]*websocket.Conn // conexões específicas por instância
	broadcast     []*websocket.Conn          // conexões que recebem todos os eventos
	clientsMux    sync.RWMutex
	writeMux      sync.Mutex // serializa as escritas no WebSocket (gorilla não permite writes concorrentes)
	loggerWrapper *logger_wrapper.LoggerManager
}

func NewWebsocketProducer(loggerWrapper *logger_wrapper.LoggerManager) *websocketProducer {
	return &websocketProducer{
		clients:       make(map[string]*websocket.Conn),
		broadcast:     make([]*websocket.Conn, 0),
		clientsMux:    sync.RWMutex{},
		loggerWrapper: loggerWrapper,
	}
}

// ServeWs lida com as requisições de upgrade para websocket
func ServeWs(w http.ResponseWriter, r *http.Request, instanceId string, producer *websocketProducer) {
	logger.LogInfo("Iniciando upgrade da conexão WebSocket")
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.LogError("Erro ao fazer upgrade da conexão websocket: %v", err)
		return
	}

	logger.LogInfo("Conexão WebSocket estabelecida com sucesso")

	if instanceId == "" {
		producer.AddBroadcastClient(conn)
	} else {
		producer.AddClient(instanceId, conn)
	}

	// Goroutine para limpar conexão quando fechada
	go func() {
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				if instanceId == "" {
					producer.RemoveBroadcastClient(conn)
				} else {
					producer.RemoveClient(instanceId)
				}
				conn.Close()
				break
			}
		}
	}()
}

func (p *websocketProducer) AddBroadcastClient(conn *websocket.Conn) {
	p.clientsMux.Lock()
	defer p.clientsMux.Unlock()
	p.broadcast = append(p.broadcast, conn)
	logger.LogInfo("Cliente broadcast websocket adicionado")
}

func (p *websocketProducer) RemoveBroadcastClient(conn *websocket.Conn) {
	p.clientsMux.Lock()
	defer p.clientsMux.Unlock()
	for i, c := range p.broadcast {
		if c == conn {
			p.broadcast = append(p.broadcast[:i], p.broadcast[i+1:]...)
			break
		}
	}
	logger.LogInfo("Cliente broadcast websocket removido")
}

func (p *websocketProducer) AddClient(instanceID string, conn *websocket.Conn) {
	p.clientsMux.Lock()
	defer p.clientsMux.Unlock()
	p.clients[instanceID] = conn
	p.loggerWrapper.GetLogger(instanceID).LogInfo("Cliente websocket adicionado para instância: %s", instanceID)
}

func (p *websocketProducer) RemoveClient(instanceID string) {
	p.clientsMux.Lock()
	defer p.clientsMux.Unlock()
	delete(p.clients, instanceID)
	p.loggerWrapper.GetLogger(instanceID).LogInfo("Cliente websocket removido para instância: %s", instanceID)
}

func (p *websocketProducer) Produce(queueName string, payload []byte, instanceID string, _ string) error {
	message := map[string]interface{}{
		"queue":   strings.ToLower(queueName),
		"payload": string(payload),
	}

	// Tomamos as referências sob RLock e o liberamos ANTES de escrever na rede,
	// para não bloquear Add/Remove durante o I/O.
	p.clientsMux.RLock()
	client, exists := p.clients[instanceID]
	broadcast := append([]*websocket.Conn(nil), p.broadcast...)
	p.clientsMux.RUnlock()

	// Um único escritor por vez: evita o panic de escrita concorrente no WebSocket.
	p.writeMux.Lock()
	defer p.writeMux.Unlock()

	// Envia para cliente específico da instância
	if exists {
		if err := client.WriteJSON(message); err != nil {
			p.loggerWrapper.GetLogger(instanceID).LogError("Erro ao enviar mensagem websocket para %s: %v", instanceID, err)
			return err
		}
		p.loggerWrapper.GetLogger(instanceID).LogInfo("Mensagem websocket enviada com sucesso para instância %s na fila %s", instanceID, queueName)
	}

	// Envia para todos os clientes broadcast
	for _, conn := range broadcast {
		if err := conn.WriteJSON(message); err != nil {
			p.loggerWrapper.GetLogger(instanceID).LogError("Erro ao enviar mensagem broadcast websocket: %v", err)
			continue
		}
	}

	return nil
}

// CreateGlobalQueues não faz nada para websocket producer
func (p *websocketProducer) CreateGlobalQueues() error {
	return nil
}
