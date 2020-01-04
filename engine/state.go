package engine

import (
	"TL-Data-Consumer/domain"
	"TL-Data-Consumer/entity"
	"strings"
	"time"
)

const (
	// RFC3339Milli unified time format, refer to time.RFC3339Nano
	RFC3339Milli = "2006-01-02T15:04:05.999Z07:00"
)

func getUserID(key string) string {
	return strings.Split(key, ";")[0]
}

func (e *Engine) buildState(h *entity.Header) (*domain.State, error) {
	userid := getUserID(h.Key)

	// parse time with unified format
	t, err := time.Parse(RFC3339Milli, h.Time)
	if err != nil {
		return nil, err
	}
	return &domain.State{
		UserID:    userid,
		Source:    h.Source,
		Path:      h.Path,
		Type:      "status",
		Value:     h.Data,
		Time:      t,
		Timestamp: time.Now(),
	}, nil
}

func (e *Engine) updateState(state *domain.State) []domain.Schema {
	e.stateMux.Lock()
	defer e.stateMux.Unlock()

	// append the state to buffer
	e.states = append(e.states, state)
	// check if the slice reaches the fixed length
	if len(e.states) == e.settings.Server.EngineBatch {
		buf := e.states
		// buffer and clear the old buffer
		e.states = make([]domain.Schema, 0, e.settings.Server.EngineBatch)

		return buf
	}

	return nil
}
